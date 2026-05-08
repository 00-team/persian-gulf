use rustls::pki_types::InvalidDnsNameError;
use shared::shipment::BinDencode;
use shared::spring::Spring;
use shared::uid::UniqueId;
use shared::utils::{Buffer, SocksHost};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum EverError {
    SocksVersionMismatch,
    SocksNoAcceptableMethod,
    SocksAuthRequired,
    // SocksBadAuth,
    SocksInvalidConnect,
    // SocksUnsupportedAddress,
    InvalidDnsNameError,
    RequestFailed,
    ConnectionFailed,
    ReadTimeout,
    Eof,
    InvalidHttpResponse,
    InvalidGzip,
    InvalidBody,
    #[allow(dead_code)]
    Io(std::io::Error),
}

impl From<InvalidDnsNameError> for EverError {
    fn from(_: InvalidDnsNameError) -> Self {
        Self::InvalidDnsNameError
    }
}

impl From<std::io::Error> for EverError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub struct SocksChannelCold {
    s: TcpStream,
    index: u64,
    pub host: SocksHost,
    pub port: u16,
}

impl SocksChannelCold {
    const SOCKS_VERSION: u8 = 0x05;
    const CMD_CONNECT: u8 = 0x01;

    const METHOD_NO_AUTH: u8 = 0x00;
    const METHOD_USER_PASS: u8 = 0x02;

    const AUTH_VERSION: u8 = 0x01;
    const AUTH_SUCCESS: u8 = 0x00;
    // const AUTH_FAILURE: u8 = 0x01;

    pub async fn init(
        stream: TcpStream, index: u64,
    ) -> Result<Self, EverError> {
        let mut sc =
            Self { s: stream, index, host: SocksHost::Ipv4([0; 4]), port: 0 };

        sc.handshake().await?;
        sc.target().await?;

        Ok(sc)
    }

    async fn handshake(&mut self) -> Result<(), EverError> {
        // socks version , number of methods
        let mut header = [0u8; 2];
        self.s.read_exact(&mut header).await?;
        if header[0] != Self::SOCKS_VERSION {
            return Err(EverError::SocksVersionMismatch);
        }

        let nmethods = header[1] as usize;
        let mut methods = [0u8; 255];
        self.s.read_exact(&mut methods[..nmethods]).await?;

        if methods.contains(&Self::METHOD_NO_AUTH) {
            self.s
                .write_all(&[Self::SOCKS_VERSION, Self::METHOD_NO_AUTH])
                .await?;
            return Ok(());
        }

        if !methods.contains(&Self::METHOD_USER_PASS) {
            self.s.write_all(&[Self::SOCKS_VERSION, 0xFF]).await?;
            return Err(EverError::SocksNoAcceptableMethod);
        }

        self.s
            .write_all(&[Self::SOCKS_VERSION, Self::METHOD_USER_PASS])
            .await?;

        // auth method, username len
        let mut auth_header = [0u8; 2];
        self.s.read_exact(&mut auth_header).await?;

        if auth_header[0] != Self::AUTH_VERSION {
            return Err(EverError::SocksAuthRequired);
        }

        let ulen = auth_header[1] as usize;
        let mut username = [0u8; 255];
        self.s.read_exact(&mut username[..ulen]).await?;

        let mut plen_buf = [0u8; 1];
        self.s.read_exact(&mut plen_buf).await?;
        let plen = plen_buf[0] as usize;

        let mut password = [0u8; 255];
        self.s.read_exact(&mut password[..plen]).await?;

        log::debug!(
            "user: {:?} | pass: {:?}",
            str::from_utf8(&username[..ulen]),
            str::from_utf8(&password[..plen])
        );

        self.s.write_all(&[Self::AUTH_VERSION, Self::AUTH_SUCCESS]).await?;
        // TODO: handle userpass correctly
        // if user_ok && pass_ok {
        // } else {
        //     client.write_all(&[AUTH_VERSION, AUTH_FAILURE])?;
        //     return Err(io::Error::new(
        //         ErrorKind::PermissionDenied,
        //         "bad credentials",
        //     ));
        // }

        Ok(())
    }

    async fn target(&mut self) -> Result<(), EverError> {
        // socks version, command, RSV, ATYP
        let mut req_header = [0u8; 3];
        self.s.read_exact(&mut req_header).await?;
        if req_header[0] != Self::SOCKS_VERSION
            || req_header[1] != Self::CMD_CONNECT
            || req_header[2] != 0
        {
            return Err(EverError::SocksInvalidConnect);
        }

        let host = SocksHost::read(&mut self.s).await?;

        let mut port_buf = [0u8; 2];
        self.s.read_exact(&mut port_buf).await?;
        let port = u16::from_be_bytes(port_buf);

        self.host = host;
        self.port = port;

        // assume Success reply
        self.s
            .write_all(&[
                Self::SOCKS_VERSION,
                0x00, // succeeded
                0x00,
                SocksHost::ATYP_IPV4,
                0,
                0,
                0,
                0, // BND.ADDR 0.0.0.0
                0,
                0, // BND.PORT 0
            ])
            .await?;

        Ok(())
    }

    pub fn run(self) -> Spring {
        let ended = Arc::new(AtomicBool::new(false));

        let (sx_alzahra, rx_alzahra) = mpsc::channel::<Buffer>(1024);
        let (sx_channel, rx_channel) = mpsc::channel::<Buffer>(1024);

        let runner = Spring {
            id: UniqueId::new(self.index),
            host: self.host,
            port: self.port,
            sx: sx_channel,
            rx: rx_alzahra,
            ended: ended.clone(),
        };

        let (tcp_read, tcp_write) = self.s.into_split();
        tokio::spawn(Self::read_loop(tcp_read, sx_alzahra, ended.clone()));
        tokio::spawn(Self::write_loop(tcp_write, rx_channel, ended.clone()));

        runner
    }

    async fn read_loop(
        mut stream: OwnedReadHalf, sx_alzahra: mpsc::Sender<Buffer>,
        ended: Arc<AtomicBool>,
    ) {
        let mut buf = [0u8; Buffer::LEN];

        while !ended.load(Ordering::Relaxed) {
            match stream.read(&mut buf).await {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    if sx_alzahra.send(Buffer::new(&buf[..n])).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    log::error!("read error: {e:#?}");
                    break;
                }
            };
        }

        ended.store(true, Ordering::SeqCst);
    }

    async fn write_loop(
        mut stream: OwnedWriteHalf, mut rx_channel: mpsc::Receiver<Buffer>,
        ended: Arc<AtomicBool>,
    ) {
        while !ended.load(Ordering::Relaxed) {
            let Some(data) = rx_channel.recv().await else { return };
            if stream.write_all(data.read()).await.is_err() {
                ended.store(true, Ordering::SeqCst);
            }
        }
    }
}

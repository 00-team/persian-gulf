use rustls::pki_types::InvalidDnsNameError;
use shared::shipment::BinDencode;
use shared::spring::Spring;
use shared::uid::UniqueId;
use shared::utils::SocksHost;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

// use crate::config::Config;

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
    udp: Option<UdpSocket>,
    user: Option<String>,
}

impl SocksChannelCold {
    const SOCKS_VERSION: u8 = 0x05;
    const CMD_CONNECT: u8 = 0x01;
    const CMD_UDP_CONNECT: u8 = 0x03;

    const METHOD_NO_AUTH: u8 = 0x00;
    const METHOD_USER_PASS: u8 = 0x02;

    // const AUTH_VERSION: u8 = 0x01;
    // const AUTH_SUCCESS: u8 = 0x00;
    // const AUTH_FAILURE: u8 = 0x01;

    pub async fn init(
        stream: TcpStream, index: u64,
    ) -> Result<Self, EverError> {
        let mut sc = Self {
            s: stream,
            index,
            host: SocksHost::Ipv4([0; 4]),
            port: 0,
            udp: None,
            user: None,
        };

        sc.handshake().await?;
        sc.target().await?;

        Ok(sc)
    }

    async fn handshake(&mut self) -> Result<(), EverError> {
        // socks version , number of methods
        let mut header = [0u8; 2];
        self.s.read_exact(&mut header).await?;
        if header[0] != Self::SOCKS_VERSION {
            log::error!("invalid version");
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

        // TODO: for some reason when auth is required only some connections
        // work. and after some times they die
        Err(EverError::SocksAuthRequired)

        // self.s
        //     .write_all(&[Self::SOCKS_VERSION, Self::METHOD_USER_PASS])
        //     .await?;
        //
        // // auth method, username len
        // let mut auth_header = [0u8; 2];
        // self.s.read_exact(&mut auth_header).await?;
        //
        // if auth_header[0] != Self::AUTH_VERSION {
        //     return Err(EverError::SocksAuthRequired);
        // }
        //
        // let ulen = auth_header[1] as usize;
        // let mut username = [0u8; 255];
        // self.s.read_exact(&mut username[..ulen]).await?;
        //
        // let mut plen_buf = [0u8; 1];
        // self.s.read_exact(&mut plen_buf).await?;
        // let plen = plen_buf[0] as usize;
        //
        // let mut password = [0u8; 255];
        // self.s.read_exact(&mut password[..plen]).await?;
        //
        // let user = str::from_utf8(&username[..ulen]).unwrap_or("");
        // let pass = str::from_utf8(&password[..plen]).unwrap_or("");
        //
        // let conf = Config::get();
        // if !conf.users.get(user).map(|p| p == pass).unwrap_or(false) {
        //     self.s.write_all(&[Self::AUTH_VERSION, Self::AUTH_FAILURE]).await?;
        //     log::warn!("invalid password for user: {user}");
        //     return Err(EverError::SocksBadAuth);
        // }
        //
        // self.user = Some(user.to_string());
        // self.s.write_all(&[Self::AUTH_VERSION, Self::AUTH_SUCCESS]).await?;
        // Ok(())
    }

    async fn target(&mut self) -> Result<(), EverError> {
        // socks version, command, RSV, ATYP
        let mut req_header = [0u8; 3];
        self.s.read_exact(&mut req_header).await?;
        if req_header[0] != Self::SOCKS_VERSION || req_header[2] != 0 {
            return Err(EverError::SocksInvalidConnect);
        }

        let cp = 0u16;
        if req_header[1] == Self::CMD_UDP_CONNECT {
            return Err(EverError::SocksInvalidConnect);

            // let mut addr = self.s.local_addr()?;
            // addr.set_port(0);
            // let udp = UdpSocket::bind(addr).await?;
            // cp = udp.local_addr()?.port();
            // self.udp = Some(udp);
        } else if req_header[1] == Self::CMD_CONNECT {
        } else {
            return Err(EverError::SocksInvalidConnect);
        };

        let host = SocksHost::read(&mut self.s).await?;

        let mut port_buf = [0u8; 2];
        self.s.read_exact(&mut port_buf).await?;
        let port = u16::from_be_bytes(port_buf);

        self.host = host;
        self.port = port;

        let cp = cp.to_be_bytes();

        let reply = [
            Self::SOCKS_VERSION,
            0x00, // succeeded
            0x00,
            SocksHost::ATYP_IPV4,
            0,
            0,
            0,
            0, // BND.ADDR 0.0.0.0
            cp[0],
            cp[1],
        ];

        self.s.write_all(&reply).await?;

        Ok(())
    }

    pub fn run(self) -> Spring {
        let ended = Arc::new(AtomicBool::new(false));

        let (sx_alzahra, rx_alzahra) = mpsc::channel::<Vec<u8>>(2048);
        let (sx_channel, rx_channel) = mpsc::channel::<Vec<u8>>(2048);
        let data = Arc::new(Mutex::new(Vec::new()));

        let runner = Spring {
            id: UniqueId::new(self.index),
            host: self.host,
            port: self.port,
            sx: sx_channel,
            data: data.clone(),
            ended: ended.clone(),
            user: self.user,
        };

        tokio::spawn(Self::debt_collector(rx_alzahra, data));

        if let Some(udp) = self.udp {
            tokio::spawn(Self::udp_loop(self.s, udp, ended.clone()));
            return runner;
        }

        let (tcp_read, tcp_write) = self.s.into_split();
        tokio::spawn(Self::read_loop(tcp_read, sx_alzahra, ended.clone()));
        tokio::spawn(Self::write_loop(tcp_write, rx_channel, ended.clone()));

        runner
    }

    async fn debt_collector(
        mut rx: mpsc::Receiver<Vec<u8>>, data: Arc<Mutex<Vec<u8>>>,
    ) {
        while let Some(chunk) = rx.recv().await {
            data.lock().await.extend_from_slice(&chunk);
        }
    }

    async fn udp_loop(
        mut tcp: TcpStream, udp: UdpSocket, ended: Arc<AtomicBool>,
    ) {
        let mut tcp_buf = [0u8; 1]; // just to detect EOF
        let mut udp_buf = vec![0u8; 65535];

        while !ended.load(Ordering::Relaxed) {
            tokio::select! {
                read_res = tcp.read(&mut tcp_buf) => {
                    if read_res.map(|n| n == 0).unwrap_or(true) {
                        break;
                    }
                }
                udp_res = udp.recv_from(&mut udp_buf) => {
                    let (len, sender) = match udp_res {
                        Ok(r) => r,
                        Err(_) => break,
                    };

                    let data = &udp_buf[..len];
                    log::info!("udp: {sender:?}\n\n{data:?}");

                    // if let Some(client) = client_addr {
                    //     if sender == client {
                    //         // Packet from the client -> forward to target
                    //         if let Err(_) = forward_request(&udp_socket, data, client).await {
                    //             break;
                    //         }
                    //     } else {
                    //         // Packet from a target -> wrap & send back to client
                    //         if let Err(_) = send_reply(&udp_socket, data, sender, client).await {
                    //             break;
                    //         }
                    //     }
                    // } else {
                    //     // First datagram – assume it comes from the client.
                    //     // You could add extra validation (check SOCKS5 header) but in practice
                    //     // the first one will be from the client.
                    //     client_addr = Some(sender);
                    //     if let Err(_) = forward_request(&udp_socket, data, sender).await {
                    //         break;
                    //     }
                    // }


                }
            }
        }
    }

    async fn read_loop(
        mut stream: OwnedReadHalf, sx_alzahra: mpsc::Sender<Vec<u8>>,
        ended: Arc<AtomicBool>,
    ) {
        let mut buf = vec![0u8; 65536];

        while !ended.load(Ordering::Relaxed) {
            match stream.read(&mut buf).await {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    if sx_alzahra.send(buf[..n].to_vec()).await.is_err() {
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
        mut stream: OwnedWriteHalf, mut rx_channel: mpsc::Receiver<Vec<u8>>,
        ended: Arc<AtomicBool>,
    ) {
        while !ended.load(Ordering::Relaxed) {
            let Some(data) = rx_channel.recv().await else { return };
            if stream.write_all(&data).await.is_err() {
                ended.store(true, Ordering::SeqCst);
            }
        }
    }
}

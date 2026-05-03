use std::io::{Read, Write};
use std::net::TcpStream;

#[derive(Debug)]
pub enum EverError {
    SocksVersionMismatch,
    SocksNoAcceptableMethod,
    SocksAuthRequired,
    SocksBadAuth,
    SocksInvalidConnect,
    SocksUnsupportedAddress,
    Io(std::io::Error),
}

impl From<std::io::Error> for EverError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug)]
pub enum Host {
    Ipv4([u8; 4]),
    Ipv6([u8; 16]),
    Domain { domain: [u8; 255], len: u8 },
}

pub struct SocksClient {
    s: TcpStream,
    pub host: Host,
    pub port: u16,
}

impl SocksClient {
    const SOCKS_VERSION: u8 = 0x05;
    const CMD_CONNECT: u8 = 0x01;
    const ATYP_IPV4: u8 = 0x01;
    const ATYP_DOMAIN: u8 = 0x03;
    const ATYP_IPV6: u8 = 0x04;

    const METHOD_NO_AUTH: u8 = 0x00;
    const METHOD_USER_PASS: u8 = 0x02;

    const AUTH_VERSION: u8 = 0x01;
    const AUTH_SUCCESS: u8 = 0x00;
    const AUTH_FAILURE: u8 = 0x01;

    pub fn init(stream: TcpStream) -> Result<Self, EverError> {
        let mut sc = Self { s: stream, host: Host::Ipv4([0; 4]), port: 0 };

        sc.handshake()?;
        sc.target()?;

        Ok(sc)
    }

    fn handshake(&mut self) -> Result<(), EverError> {
        // socks version , number of methods
        let mut header = [0u8; 2];
        self.s.read_exact(&mut header)?;
        if header[0] != Self::SOCKS_VERSION {
            return Err(EverError::SocksVersionMismatch);
        }

        let nmethods = header[1] as usize;
        let mut methods = [0u8; 255];
        self.s.read_exact(&mut methods[..nmethods])?;

        if !methods.contains(&Self::METHOD_USER_PASS) {
            self.s.write_all(&[Self::SOCKS_VERSION, 0xFF])?;
            return Err(EverError::SocksNoAcceptableMethod);
        }

        self.s.write_all(&[Self::SOCKS_VERSION, Self::METHOD_USER_PASS])?;

        // auth method, username len
        let mut auth_header = [0u8; 2];
        self.s.read_exact(&mut auth_header)?;

        if auth_header[0] != Self::AUTH_VERSION {
            return Err(EverError::SocksAuthRequired);
        }

        let ulen = auth_header[1] as usize;
        let mut username = [0u8; 255];
        self.s.read_exact(&mut username[..ulen])?;

        let mut plen_buf = [0u8; 1];
        self.s.read_exact(&mut plen_buf)?;
        let plen = plen_buf[0] as usize;

        let mut password = [0u8; 255];
        self.s.read_exact(&mut password[..plen])?;

        println!(
            "user: {:?} | pass: {:?}",
            str::from_utf8(&username[..ulen]),
            str::from_utf8(&password[..plen])
        );

        self.s.write_all(&[Self::AUTH_VERSION, Self::AUTH_SUCCESS])?;
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

    fn target(&mut self) -> Result<(), EverError> {
        // socks version, command, RSV, ATYP
        let mut req_header = [0u8; 4];
        self.s.read_exact(&mut req_header)?;
        if req_header[0] != Self::SOCKS_VERSION
            || req_header[1] != Self::CMD_CONNECT
            || req_header[2] != 0
        {
            return Err(EverError::SocksInvalidConnect);
        }

        let atyp = req_header[3];
        let host = match atyp {
            Self::ATYP_IPV4 => {
                let mut ip = [0u8; 4];
                self.s.read_exact(&mut ip)?;
                Host::Ipv4(ip)
            }
            Self::ATYP_DOMAIN => {
                let mut len_buf = [0u8; 1];
                self.s.read_exact(&mut len_buf)?;
                let len = len_buf[0];
                let mut domain = [0u8; 255];
                self.s.read_exact(&mut domain[..len as usize])?;
                println!("domain: {:?}", str::from_utf8(&domain));
                Host::Domain { domain, len }
            }
            Self::ATYP_IPV6 => {
                let mut ip = [0u8; 16];
                self.s.read_exact(&mut ip)?;
                Host::Ipv6(ip)
            }
            _ => {
                return Err(EverError::SocksUnsupportedAddress);
            }
        };

        let mut port_buf = [0u8; 2];
        self.s.read_exact(&mut port_buf)?;
        let port = u16::from_be_bytes(port_buf);

        self.host = host;
        self.port = port;

        // Resolve and connect to the target
        // let target = format!("{}:{}", addr, port)
        //     .to_socket_addrs()?
        //     .next()
        //     .ok_or_else(|| {
        //         io::Error::new(ErrorKind::NotFound, "could not resolve address")
        //     })?;

        // let remote = match TcpStream::connect(target) {
        //     Ok(s) => s,
        //     Err(e) => {
        //         let _ = client.write_all(&[
        //             SOCKS_VERSION,
        //             0x04, // host unreachable
        //             0x00,
        //             ATYP_IPV4,
        //             0,
        //             0,
        //             0,
        //             0,
        //             0,
        //             0,
        //         ]);
        //         return Err(e);
        //     }
        // };

        // assume Success reply
        self.s.write_all(&[
            Self::SOCKS_VERSION,
            0x00, // succeeded
            0x00,
            Self::ATYP_IPV4,
            0,
            0,
            0,
            0, // BND.ADDR 0.0.0.0
            0,
            0, // BND.PORT 0
        ])?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Parse and execute a CONNECT request.
// Returns the connected remote stream.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Accumulator thread (std‑only).
// Uses a blocking reader with a short timeout to gather data for `window` ms.
// Then writes everything with a blocking write_all.
// ---------------------------------------------------------------------------
// fn relay_with_buffering<R: Read, W: Write>(
//     mut reader: R, mut writer: W,
// ) -> io::Result<()> {
//     let window = Duration::from_millis(500);
//     let read_timeout = Duration::from_millis(10); // check every 10ms
//     let mut buf = [0u8; 8192];
//
//     loop {
//         let start = Instant::now();
//         let mut accumulated = Vec::new();
//
//         // --- gather phase (up to `window` ms or EOF) ---
//         loop {
//             match reader.read(&mut buf) {
//                 Ok(0) => {
//                     // EOF – flush any remaining data and exit
//                     if !accumulated.is_empty() {
//                         writer.write_all(&accumulated)?;
//                     }
//                     return Ok(());
//                 }
//                 Ok(n) => {
//                     accumulated.extend_from_slice(&buf[..n]);
//                     // Got data – immediately try to read more without checking
//                     // the window; we only check the elapsed time when a read
//                     // times out.
//                     continue;
//                 }
//                 Err(ref e)
//                     if e.kind() == ErrorKind::WouldBlock
//                         || e.kind() == ErrorKind::TimedOut =>
//                 {
//                     // No data right now. If the window has expired, flush.
//                     if start.elapsed() >= window {
//                         break; // exit gather loop, flush below
//                     }
//                     // Otherwise just loop back and try another read.
//                     // The next read will again block at most `read_timeout`.
//                 }
//                 Err(e) => return Err(e),
//             }
//         }
//
//         // --- flush phase ---
//         if !accumulated.is_empty() {
//             writer.write_all(&accumulated)?;
//             // write_all already ensures everything is sent.
//             // Optionally call writer.flush()? here.
//         }
//         // Loop back and start a fresh 500ms window.
//     }
// }
//
// // ---------------------------------------------------------------------------
// // Handle one client connection
// // ---------------------------------------------------------------------------
// fn handle_client(mut client: TcpStream) -> io::Result<()> {
//     // 1. Handshake + authentication (blocking, no special settings needed)
//     do_handshake(&mut client)?;
//
//     // 2. CONNECT request → obtain remote stream
//     let remote = handle_connect(&mut client)?;
//
//     // 3. Clone both streams. Each clone will be used for one direction.
//     //    The original `client` and `remote` are dropped; we use the clones.
//     let client_reader = client.try_clone()?;
//     let client_writer = client; // reuse the original handle for writing
//
//     let remote_reader = remote.try_clone()?;
//     let remote_writer = remote;
//
//     // 4. Set a short read timeout on the *reader* clones.
//     //    (The writer clones stay with the default infinite timeout, so
//     //    write_all behaves normally and won't spin on WouldBlock.)
//     client_reader.set_read_timeout(Some(Duration::from_millis(10)))?;
//     remote_reader.set_read_timeout(Some(Duration::from_millis(10)))?;
//
//     // 5. Spawn two relay threads.
//     let t1 = thread::spawn(move || {
//         if let Err(e) = relay_with_buffering(client_reader, remote_writer) {
//             eprintln!("client→remote relay error: {}", e);
//         }
//     });
//
//     let t2 = thread::spawn(move || {
//         if let Err(e) = relay_with_buffering(remote_reader, client_writer) {
//             eprintln!("remote→client relay error: {}", e);
//         }
//     });
//
//     let _ = t1.join();
//     let _ = t2.join();
//
//     Ok(())
// }
//
// fn main() -> io::Result<()> {
//     let listener = TcpListener::bind("127.0.0.1:1080")?;
//     println!("SOCKS5 proxy listening on 127.0.0.1:1080");
//
//     for stream in listener.incoming() {
//         match stream {
//             Ok(client) => {
//                 thread::spawn(|| {
//                     if let Err(e) = handle_client(client) {
//                         eprintln!("client handler error: {}", e);
//                     }
//                 });
//             }
//             Err(e) => eprintln!("accept error: {}", e),
//         }
//     }
//
//     Ok(())
// }

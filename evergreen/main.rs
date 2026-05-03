use std::{io, net::TcpListener};

use crate::socks::SocksClient;

mod socks;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6007")?;
    listener.set_nonblocking(true);

    println!("socks on: 127.0.0.1:6007");

    let mut clients = Vec::with_capacity(200);

    for stream in listener.incoming() {
        for sc in clients.iter_mut() {
        }



        let stream = match stream {
            Ok(v) => v,
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock => continue,
                _ => {
                    println!("\x1b[31mERR\x1b[m: stream error: {e:#?}");
                    continue;
                }
            },
        };
        
        let sc = match SocksClient::init(stream) {
            Ok(v) => v,
            Err(e) => {
                println!("\x1b[31msocks client error\x1b[m: {e:?}");
                continue;
            }
        };
        println!("host: {:?}:{}", sc.host, sc.port);
        clients.push(sc);
    }

    Ok(())
}

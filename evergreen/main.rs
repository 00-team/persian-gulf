use crate::socks::{SocksChannelCold, SocksChannelRunning};
use shared::{
    logger,
    shipment::{BinDencode, Shipment, ShipmentChannel},
    uid::UniqueId,
};
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::Mutex;
use tokio::{net::TcpListener, sync::mpsc};

mod socks;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    log::set_logger(&logger::MasterLogger).expect("could not init logger");
    log::set_max_level(log::LevelFilter::Trace);

    let listener = TcpListener::bind("127.0.0.1:6007").await?;
    log::info!("socks on: 127.0.0.1:6007");

    let socks_channels =
        Arc::new(Mutex::new(Vec::<SocksChannelRunning>::with_capacity(200)));
    let mut channel_count = 0;

    let sca = socks_channels.clone();
    let h_shiper = tokio::task::spawn(shiper(socks_channels.clone()));
    let timeout = std::time::Duration::from_secs(30);

    while let Ok((stream, addr)) = listener.accept().await {
        // let Ok(stream) = stream else {
        //     log::error!("stream error");
        //     continue;
        // };
        // stream.set_read_timeout(Some(timeout)).unwrap();
        // stream.set_write_timeout(Some(timeout)).unwrap();
        // let stream = match stream {
        //     Ok(v) => v,
        //     Err(e) => match e.kind() {
        //         io::ErrorKind::WouldBlock => continue,
        //         _ => {
        //             log::error!("\x1b[31mERR\x1b[m: stream error: {e:#?}");
        //             continue;
        //         }
        //     },
        // };

        channel_count += 1;
        let scc = match SocksChannelCold::init(stream, channel_count).await {
            Ok(v) => v,
            Err(e) => {
                log::error!("\x1b[31msocks client error\x1b[m: {e:?}");
                continue;
            }
        };
        log::debug!("host: {:?}:{}", scc.host, scc.port);
        socks_channels.lock().await.push(scc.run());
    }

    h_shiper.await?;

    Ok(())
}

async fn shiper(socks_channels: Arc<Mutex<Vec<SocksChannelRunning>>>) {
    let ship_uid = UniqueId::new(77);
    let ship_sleep = std::time::Duration::from_secs(1);
    let client = reqwest::Client::builder().build().unwrap();
    const SHIP_URL: &str = "http://localhost:7707/api/proxy/bin-batch/";
    let mut order = 0;

    loop {
        let channels = {
            let mut mg = socks_channels.lock().await;
            let mut channels = Vec::with_capacity(mg.len());

            mg.retain_mut(|c| {
                if c.ended.load(Ordering::Relaxed) {
                    return false;
                }

                let mut data = Vec::with_capacity(100 * 1024);

                loop {
                    match c.rx.try_recv() {
                        Ok(v) => {
                            data.extend_from_slice(v.read());
                            if v.is_full() {
                                continue;
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            return false;
                        }
                    }

                    break;
                }

                if data.is_empty() {
                    return true;
                }

                channels.push(ShipmentChannel {
                    id: c.id,
                    port: c.port,
                    host: c.host.clone(),
                    data,
                });

                true
            });

            channels
        };
        if channels.is_empty() {
            std::thread::sleep(std::time::Duration::from_millis(100));
            continue;
        }

        order += 1;
        let shipment = Shipment { ship_uid, order, channels };

        let mut cursor =
            std::io::Cursor::new(Vec::with_capacity(1 * 1024 * 1024));
        shipment.write(&mut cursor).await.unwrap();
        log::info!("sending ship: {shipment:?}");
        let rq = client.post(SHIP_URL).body(cursor.into_inner()).send().await;
        log::info!("result: {rq:?}");

        log::debug!("\x1b[32mship delay\x1b[m");
        std::thread::sleep(ship_sleep);
    }
}

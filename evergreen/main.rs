use crate::config::Config;
use crate::socks::SocksChannelCold;
use base64::Engine;
use shared::shipment::{BinDencode, SpringTank};
use shared::{logger, shipment::Shipment, spring::Spring, uid::UniqueId};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

mod config;
mod fronter;
mod socks;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    log::set_logger(&logger::MasterLogger).expect("could not init logger");
    log::set_max_level(log::LevelFilter::Trace);

    let conf = Config::get();

    let listener = TcpListener::bind(&conf.socks_bind).await?;
    log::info!("socks on: {}", conf.socks_bind);

    let socks_channels =
        Arc::new(Mutex::new(HashMap::<UniqueId, Spring>::with_capacity(200)));
    let mut channel_count = 0;

    let h_shiper = tokio::task::spawn(shiper(socks_channels.clone()));
    // let timeout = std::time::Duration::from_secs(30);

    while let Ok((stream, _)) = listener.accept().await {
        channel_count += 1;
        let scc = match SocksChannelCold::init(stream, channel_count).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        log::debug!("host: {:?}:{}", scc.host, scc.port);
        let spring = scc.run();
        socks_channels.lock().await.insert(spring.id, spring);
    }

    h_shiper.await?;

    Ok(())
}

async fn shiper(springs: Arc<Mutex<HashMap<UniqueId, Spring>>>) {
    let mut ship_id = UniqueId::new(77);

    let conf = Config::get();
    let mut fronter =
        fronter::Fronter::new(&conf.alzahra, conf.script_ids.clone());

    let mut order = 0;
    let mut response_order = 1;
    let mut queued_shipments = Vec::with_capacity(10);

    loop {
        let mut channels = HashMap::<UniqueId, SpringTank>::with_capacity(512);

        let data_collection = Instant::now();
        let mut running_springs = 0;
        let mut data_collected_len = 0;
        loop {
            let mut mg = springs.lock().await;
            if mg.is_empty() {
                break;
            }

            mg.retain(|_, s| {
                if data_collected_len >= 30 * 1024 * 1024 {
                    return true;
                }

                if let Some(ch) = channels.get_mut(&s.id) {
                    let before = ch.data.len();
                    let ended = s.ended.load(Ordering::Relaxed)
                        || s.read_data(&mut ch.data);
                    data_collected_len += ch.data.len().saturating_sub(before);
                    ch.ended = ended;
                    return !ended;
                }

                let tank = s.to_tank();
                data_collected_len += tank.data.len();
                let ended = tank.ended;
                if ended || !tank.data.is_empty() {
                    channels.insert(s.id, tank);
                }
                !ended
            });

            running_springs = mg.len();
            if data_collection.elapsed().as_millis() >= 1500
                || data_collected_len >= 30 * 1024 * 1024
            {
                break;
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        if channels.is_empty() && running_springs == 0 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        }

        order += 1;
        let shipment = Shipment {
            ship_id,
            reset: false,
            order_request: response_order,
            order,
            tanks: channels.into_values().collect(),
        };

        let body = shipment.to_bytes().await;
        let b64_encoded = conf.b64.encode(body);

        log::info!(
            "<- {order}: {} | {}",
            shipment.tanks.len(),
            b64_encoded.len(),
        );

        let data = loop {
            match fronter.relay(b64_encoded.clone()).await {
                Ok(v) => break v,
                Err(e) => {
                    log::error!("request error: {e:?}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
        };

        let Ok(data) = conf.b64.decode(data) else {
            log::error!("invalid base64");
            continue;
        };

        let data_len = data.len();
        let mut reader = Cursor::new(data);
        let Ok(shipment) = Shipment::read(&mut reader).await else {
            log::error!("invalid shipment");
            continue;
        };

        assert_eq!(shipment.ship_id, ship_id);

        if shipment.reset {
            ship_id = UniqueId::new(99);
            order = 0;
            response_order = 1;
            queued_shipments.clear();
            springs.lock().await.clear();
            continue;
        }

        if shipment.order < response_order {
            log::error!("response order mismatch");
            continue;
        }

        if shipment.order > response_order {
            queued_shipments.push(shipment);
            log::warn!("queued: {}", queued_shipments.len());
            continue;
        }

        log::info!("-> {} | {data_len}", shipment.tanks.len());
        response_order = shipment.order + 1;

        let springs = springs.lock().await;
        for tank in shipment.tanks {
            let Some(spring) = springs.get(&tank.id) else { continue };

            if spring.ended.load(Ordering::Relaxed) {
                continue;
            }

            if spring.sx.send(tank.data).await.is_err() {
                spring.ended.store(true, Ordering::Relaxed);
            }

            if tank.ended {
                spring.ended.store(true, Ordering::Relaxed);
            }
        }
    }
}

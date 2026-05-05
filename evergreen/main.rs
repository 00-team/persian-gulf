use crate::socks::SocksChannelCold;
use base64::Engine;
use shared::shipment::{BinDencode, SpringTank};
use shared::utils::Buffer;
use shared::{logger, shipment::Shipment, spring::Spring, uid::UniqueId};
use std::collections::HashMap;
use std::io::Cursor;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

mod socks;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    log::set_logger(&logger::MasterLogger).expect("could not init logger");
    log::set_max_level(log::LevelFilter::Trace);

    // log::info!("res: {res:#?}");
    // if let Ok(res) = res {
    //     log::info!("res: {:#?}", res.text().await);
    // }
    //
    // return Ok(());

    let listener = TcpListener::bind("127.0.0.1:6007").await?;
    log::info!("socks on: 127.0.0.1:6007");

    let socks_channels =
        Arc::new(Mutex::new(HashMap::<UniqueId, Spring>::with_capacity(200)));
    let mut channel_count = 0;

    let h_shiper = tokio::task::spawn(shiper(socks_channels.clone()));
    // let timeout = std::time::Duration::from_secs(30);

    while let Ok((stream, _)) = listener.accept().await {
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
    let ship_uid = UniqueId::new(77);

    let b64 = base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::GeneralPurposeConfig::new()
            .with_encode_padding(false)
            .with_decode_padding_mode(
                base64::engine::DecodePaddingMode::Indifferent,
            ),
    );

    let client = reqwest::Client::builder()
        .resolve(
            "script.google.com",
            (Ipv4Addr::new(216, 239, 38, 120), 443).into(),
        )
        .resolve(
            "script.googleusercontent.com",
            (Ipv4Addr::new(216, 239, 38, 120), 443).into(),
        )
        .pool_idle_timeout(std::time::Duration::from_secs(5))
        .pool_max_idle_per_host(0)
        .build()
        .unwrap();

    // let res = client
    //     .post(concat!(
    //         "https://script.google.com/macros/s/",
    //         "AKfycbyQoU6ub9jNPnfYqpQksFBHdjVw8MCA_",
    //         "spTAb8FgLgNNRoKvzG7MEcA0y2Xfe1dM3mI",
    //         "/dev"
    //     ))
    //     .query(&[
    //         ("t", "http://94.183.183.223:9920/api/proxy/bin-batch/"),
    //         ("a", "F1ilebxCY4vqDYkisjbOgdOf9Sw"),
    //     ])
    //     .body("hi")
    //     .send()
    //     .await;

    // log::debug!("test: {res:#?}");

    // let res = client
    //     .post(concat!(
    //         "https://script.google.com/macros/s/",
    //         "AKfycbyQoU6ub9jNPnfYqpQksFBHdjVw8MCA_",
    //         "spTAb8FgLgNNRoKvzG7MEcA0y2Xfe1dM3mI",
    //         "/dev"
    //     ))
    //     .query(&[
    //         ("t", "http://94.183.183.223:9920/api/proxy/bin-batch/"),
    //         ("a", "F1ilebxCY4vqDYkisjbOgdOf9Sw"),
    //     ])
    //     .body("some text")
    //     .send()
    //     .await;

    // auth = F1ilebxCY4vqDYkisjbOgdOf9Sw
    // AKfycbyQoU6ub9jNPnfYqpQksFBHdjVw8MCA_spTAb8FgLgNNRoKvzG7MEcA0y2Xfe1dM3mI
    // let ship_sleep = std::time::Duration::from_secs(1);
    // let client = reqwest::Client::builder().build().unwrap();
    // const SHIP_URL: &str = "http://localhost:7707/api/proxy/bin-batch/";
    let mut order = 0;
    let mut response_order = 0;
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
            ship_uid,
            order,
            tanks: channels.into_values().collect(),
        };

        let body = shipment.to_bytes().await;
        let b64_encoded = b64.encode(body);

        log::info!(
            "sending ship: {order}: {} | tanks: {}",
            b64_encoded.len(),
            shipment.tanks.len()
        );

        let res = client
            .post(concat!(
                "https://script.google.com/macros/s/",
                "AKfycbyQoU6ub9jNPnfYqpQksFBHdjVw8MCA_",
                "spTAb8FgLgNNRoKvzG7MEcA0y2Xfe1dM3mI",
                "/dev"
            ))
            .query(&[
                ("t", "http://94.183.183.223:9920/api/proxy/bin-batch/"),
                ("a", "F1ilebxCY4vqDYkisjbOgdOf9Sw"),
            ])
            .body(b64_encoded)
            .send()
            .await;

        // let res = client.post(SHIP_URL).body(body).send().await;

        let res = match res {
            Ok(v) => v,
            Err(e) => {
                order = order.saturating_sub(1);
                log::error!("request error: {e:?}");
                continue;
            }
        };

        if res.status() != 200 {
            log::error!("request error: {:?}", res.text().await);
            continue;
        }

        let Ok(data) = res.text().await else {
            log::error!("error getting res bytes");
            continue;
        };

        let Ok(data) = b64.decode(data) else {
            log::error!("invalid base64");
            continue;
        };

        let mut reader = Cursor::new(data);
        let Ok(shipment) = Shipment::read(&mut reader).await else {
            log::error!("invalid shipment");
            continue;
        };

        assert_eq!(shipment.ship_uid, ship_uid);

        if shipment.order <= response_order {
            log::error!("response order mismatch");
            continue;
        }

        if shipment.order > response_order + 1 {
            queued_shipments.push(shipment);
            log::warn!("queued: {}", queued_shipments.len());
            continue;
        }

        response_order = shipment.order;

        let springs = springs.lock().await;
        for tank in shipment.tanks {
            let Some(spring) = springs.get(&tank.id) else { continue };

            if spring.ended.load(Ordering::Relaxed) {
                continue;
            }

            for buf in Buffer::from_data(&tank.data) {
                let _ = spring.sx.send(buf).await;
            }
            if tank.ended {
                spring.ended.store(true, Ordering::Relaxed);
            }
        }
    }
}

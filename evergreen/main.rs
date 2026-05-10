use crate::config::Config;
use crate::socks::SocksChannelCold;
use base64::Engine;
use shared::shipment::{BinDencode, SpringTank};
use shared::tracker::ConnectionStats;
use shared::{logger, shipment::Shipment, spring::Spring, uid::UniqueId};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore};

mod config;
mod fronter;
mod socks;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    log::set_logger(&logger::MasterLogger).expect("could not init logger");
    log::set_max_level(log::LevelFilter::Trace);

    let conf = Config::get();
    let stats = ConnectionStats::default();
    let client_stats = ConnectionStats::default();

    let listener = TcpListener::bind(&conf.socks_bind).await?;
    log::info!("socks on: {}", conf.socks_bind);

    let mut az_springs = Vec::with_capacity(conf.alzahra.len());
    let mut total_springs = 0;
    let mut next_az = 0;

    for az in conf.alzahra.iter() {
        let sps = Arc::new(Mutex::new(
            HashMap::<UniqueId, Spring>::with_capacity(200),
        ));
        let h_shiper =
            tokio::task::spawn(shiper(sps.clone(), az.clone(), stats.clone()));
        az_springs.push((h_shiper, sps));
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    let cs = client_stats.clone();
    let gs = stats.clone();
    tokio::spawn(async move {
        let mut n = 0;
        loop {
            log::debug!("client: {cs} | google: {gs}");
            tokio::time::sleep(Duration::from_secs(3)).await;
            n += 1;

            if n > 10 {
                let _ = tokio::fs::write(
                    "net-stats",
                    format!("client: {cs} | google: {gs}"),
                )
                .await;
                n = 0;
            }
        }
    });

    // let timeout = std::time::Duration::from_secs(30);

    while let Ok((stream, _)) = listener.accept().await {
        total_springs += 1;
        let scc = match SocksChannelCold::init(stream, total_springs).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let spring = scc.run(&client_stats);

        next_az += 1;
        if next_az >= az_springs.len() {
            next_az = 0;
        }

        az_springs[next_az].1.lock().await.insert(spring.id, spring);
    }

    for (t, _) in az_springs {
        let _ = t.await;
    }

    Ok(())
}

async fn shiper(
    base_springs: Arc<Mutex<HashMap<UniqueId, Spring>>>, alzahra: String,
    cs: ConnectionStats,
) {
    let name = alzahra[8..10].to_string();
    let conf = Config::get();
    let base_fronter =
        Arc::new(fronter::Fronter::new(&alzahra, conf.script_ids.clone(), cs));

    struct State {
        ship_id: UniqueId,
        order: u64,
        response_order: u64,
        queued_shipments: Vec<Shipment>,
        cycle: u64,
    }

    impl State {
        pub fn new() -> Self {
            Self {
                ship_id: UniqueId::new(77),
                order: 0,
                response_order: 0,
                cycle: 0,
                queued_shipments: Vec::with_capacity(10),
            }
        }

        pub fn reset(&mut self) {
            log::warn!("\x1b[31mRESET\x1b[m");
            self.ship_id = UniqueId::new(44);
            self.order = 0;
            self.response_order = 0;
            self.queued_shipments.clear();
            self.cycle = self.cycle.wrapping_add(1);
        }

        pub fn queue(&mut self, shipment: Shipment) -> usize {
            self.queued_shipments.push(shipment);
            self.queued_shipments.len()
        }
    }

    let base_state = Arc::new(Mutex::new(State::new()));

    async fn apply_shipment(
        springs: &mut HashMap<UniqueId, Spring>, tanks: Vec<SpringTank>,
    ) {
        for tank in tanks {
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

    let mut tasks = Vec::with_capacity(10);
    let semaphore = Arc::new(Semaphore::new(5));

    'main: loop {
        tokio::time::sleep(Duration::from_millis(conf.send_delay)).await;
        let mut tanks = HashMap::<UniqueId, SpringTank>::with_capacity(512);

        let running_springs = 'a: {
            let mut mg = base_springs.lock().await;
            if mg.is_empty() {
                break 'a 0;
            }
            let mut removal = Vec::with_capacity(mg.len());
            let mut data_collected_len = 0;
            for (sid, s) in mg.iter_mut() {
                if data_collected_len >= 15 * 1024 * 1024 {
                    break;
                }

                let tank = s.to_tank().await;
                data_collected_len += tank.data.len();
                let ended = tank.ended;
                if ended || !tank.data.is_empty() {
                    tanks.insert(s.id, tank);
                }
                if ended {
                    removal.push(*sid);
                }
            }
            for sid in removal.iter() {
                mg.remove(sid);
            }

            mg.len()
        };

        if tanks.is_empty() && running_springs == 0 {
            continue;
        }

        let qlen = { base_state.lock().await.queued_shipments.len() };
        if qlen > 7 {
            log::warn!("\x1b[93mWAITING FOR ALL TASKS\x1b[m");
            for t in tasks {
                let _ = tokio::time::timeout(Duration::from_secs(35), t).await;
            }
            tasks = Vec::with_capacity(10);
        }

        let mut state = base_state.lock().await;
        state.queued_shipments.sort_by_key(|o| o.order);
        let mut springs = base_springs.lock().await;

        if state.queued_shipments.len() > 7 {
            for q in state.queued_shipments.clone() {
                for needed in (state.response_order + 1)..q.order {
                    log::warn!("needed: {needed}");
                    let shipment = Shipment {
                        ship_id: state.ship_id,
                        order_request: needed,
                        reset: false,
                        order: 0,
                        tanks: Default::default(),
                    };

                    let bd = conf.b64.encode(shipment.to_bytes().await);
                    let bd_len = bd.len();
                    let Ok(data) = base_fronter.relay(bd).await else {
                        log::error!("qp: request failed. reset: {bd_len}");
                        state.reset();
                        springs.clear();
                        tasks.clear();
                        continue 'main;
                    };

                    let Ok(data) = conf.b64.decode(&data) else {
                        log::error!("qp: invalid base64: {data}");
                        state.reset();
                        springs.clear();
                        tasks.clear();
                        continue 'main;
                    };

                    let mut reader = Cursor::new(data);
                    let Ok(shipment) = Shipment::read(&mut reader).await else {
                        log::error!("qp: invalid shipment");
                        state.reset();
                        springs.clear();
                        tasks.clear();
                        continue 'main;
                    };

                    assert_eq!(shipment.ship_id, state.ship_id);

                    if shipment.reset {
                        log::error!("shipment reset require");
                        state.reset();
                        springs.clear();
                        tasks.clear();
                        continue 'main;
                    }

                    state.response_order = shipment.order;
                    apply_shipment(&mut springs, shipment.tanks).await;
                }

                state.response_order = q.order;
                apply_shipment(&mut springs, q.tanks).await;
            }
            state.queued_shipments.clear();
        }

        state.order += 1;
        let (order, ship_id, cycle) = (state.order, state.ship_id, state.cycle);
        let state = base_state.clone();
        let springs = base_springs.clone();
        let fronter = base_fronter.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let name = name.clone();
        tasks.push(tokio::spawn(async move {
            let _ = permit;
            let shipment = Shipment {
                ship_id,
                reset: false,
                order_request: 0,
                order,
                tanks: tanks.into_values().collect(),
            };

            let body = shipment.to_bytes().await;
            let b64_encoded = conf.b64.encode(body);

            log::info!(
                "<- \x1b[33m{order:3}\x1b[m: {:3}/{running_springs:<3} |{:7}|",
                shipment.tanks.len(),
                b64_encoded.len(),
            );

            let start = Instant::now();
            let Ok(data) = fronter.relay(b64_encoded).await else {
                let mut state = state.lock().await;
                if state.cycle == cycle {
                    log::warn!("relay failed.");
                    state.reset();
                    springs.lock().await.clear();
                }
                return;
            };
            let elapsed = start.elapsed();

            let Ok(data) = conf.b64.decode(&data) else {
                log::error!("invalid base64: {data}");
                return;
            };

            let data_len = data.len();
            let mut reader = Cursor::new(data);
            let Ok(shipment) = Shipment::read(&mut reader).await else {
                log::error!("invalid shipment");
                return;
            };

            assert_eq!(shipment.ship_id, ship_id);

            if shipment.reset {
                log::warn!("alzahra reset require");
                let mut state = state.lock().await;
                if state.cycle == cycle {
                    state.reset();
                    springs.lock().await.clear();
                }
                return;
            }

            // if shipment.order < response_order {
            //     state.lock().await.reset();
            //     springs.lock().await.clear();
            //     log::error!("response order mismatch: \x1b[31mreset\x1b[m");
            //     return;
            // }

            let mut state = state.lock().await;
            if state.cycle != cycle {
                return;
            }
            if shipment.order > state.response_order + 1 {
                let so = shipment.order;
                let q = state.queue(shipment);
                log::warn!("queued: {q} | {so} > {}", state.response_order + 1);

                return;
            }

            log::info!(
                "\x1b[33m{:<3}\x1b[m ->: {:3}/{running_springs:<3} |{data_len:7}| {name} {:.2}",
                shipment.order,
                shipment.tanks.len(),
                elapsed.as_secs_f32(),
            );

            state.response_order = shipment.order;
            let mut springs = springs.lock().await;
            apply_shipment(&mut springs, shipment.tanks).await;

            state.queued_shipments.sort_by_key(|o| o.order);
            let queued = state.queued_shipments.clone();
            let mut queue_prog = 0;
            for q in queued {
                if state.response_order + 1 != q.order {
                    break;
                }
                state.response_order = q.order;
                apply_shipment(&mut springs, q.tanks).await;
                queue_prog += 1;
            }
            state.queued_shipments.drain(0..queue_prog);
        }));
    }
}

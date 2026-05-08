use crate::config::Config;
use actix_web::web::Data;
use actix_web::{
    App, HttpServer, middleware as mw,
    web::{self, ServiceConfig, scope},
};
use shared::shipment::{Shipment, SpringTank};
use shared::spring::Spring;
use std::sync::atomic::Ordering;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

mod api;
mod config;
mod models;

pub use models::{AppErr, ErrorCode};
use shared::uid::UniqueId;

fn config_app(app: &mut ServiceConfig) {
    app.service(scope("/api").service(api::proxy::router()));

    // app.default_service(actix_web::web::to(|| async {
    //     HttpResponse::NotFound()
    // }))
}

#[derive(Debug, Default)]
pub struct Ship {
    pub springs: HashMap<UniqueId, Spring>,
    pub latest_order: u64,
    pub response_order: u64,
    pub queued_shipments: Vec<Shipment>,
    pub order_backlog: Vec<Shipment>,
}

impl Ship {
    pub fn reset(&mut self) {
        self.springs.clear();
        self.queued_shipments.clear();
        self.order_backlog.clear();
        self.latest_order = 0;
        self.response_order = 0;
    }

    pub fn new_backlog(&mut self, sm: Shipment) {
        self.order_backlog.push(sm);
        self.order_backlog.sort_by_key(|o| o.order);

        if self.order_backlog.len() > 30 {
            self.order_backlog.remove(0);
        }
    }

    pub async fn spring_update(&mut self, sm: &Shipment) {
        self.latest_order = sm.order;

        for ch in sm.tanks.iter() {
            let Some(schr) = self.springs.get(&ch.id) else {
                if ch.ended && ch.data.is_empty() {
                    continue;
                }
                self.new_channel(ch).await;
                continue;
            };

            if schr.ended.load(Ordering::Relaxed) {
                continue;
            }

            if schr.sx.send(ch.data.clone()).await.is_err() {
                schr.ended.store(true, Ordering::Relaxed);
            }

            if ch.ended {
                schr.ended.store(true, Ordering::Relaxed);
            }
        }
    }

    pub async fn new_channel(&mut self, sch: &SpringTank) {
        let ended = Arc::new(AtomicBool::new(false));

        let (sx_ship, rx_ship) = mpsc::channel::<Vec<u8>>(2048);
        let (sx_channel, rx_channel) = mpsc::channel::<Vec<u8>>(2048);

        let runner = Spring {
            id: sch.id,
            host: sch.host.clone(),
            port: sch.port,
            sx: sx_channel,
            rx: rx_ship,
            ended: ended.clone(),
        };

        self.springs.insert(sch.id, runner);

        tokio::spawn(Self::run_channel(
            sch.clone(),
            sx_ship,
            rx_channel,
            ended,
        ));
    }

    async fn run_channel(
        sch: SpringTank, sx_ship: mpsc::Sender<Vec<u8>>,
        rx_channel: mpsc::Receiver<Vec<u8>>, ended: Arc<AtomicBool>,
    ) {
        let addr = sch.host.to_addr(sch.port);
        let mut s = match TcpStream::connect(&addr).await {
            Ok(v) => v,
            Err(e) => {
                log::warn!("connect to {addr} failed: {e:?}");
                ended.store(true, Ordering::SeqCst);
                return;
            }
        };
        if let Err(e) = s.write_all(&sch.data).await {
            log::warn!("sending data to {addr} failed: {e:?}");
            ended.store(true, Ordering::SeqCst);
            return;
        }

        let (tcp_read, tcp_write) = s.into_split();
        tokio::spawn(Self::read_loop(tcp_read, sx_ship, ended.clone()));
        tokio::spawn(Self::write_loop(tcp_write, rx_channel, ended.clone()));
    }

    async fn read_loop(
        mut stream: OwnedReadHalf, sx_ship: mpsc::Sender<Vec<u8>>,
        ended: Arc<AtomicBool>,
    ) {
        let mut buf = vec![0u8; 65536];

        while !ended.load(Ordering::Relaxed) {
            match stream.read(&mut buf).await {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    log::info!("capacity: {}", sx_ship.capacity());
                    if sx_ship.send(buf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
                Err(_) => {
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
            let Some(data) = rx_channel.recv().await else { break };
            if stream.write_all(&data).await.is_err() {
                break;
            }
        }

        ended.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug, Default)]
pub struct ActiveShips {
    pub ships: Mutex<HashMap<UniqueId, Arc<Mutex<Ship>>>>,
}

impl ActiveShips {
    pub async fn ship_get(&self, id: UniqueId) -> Arc<Mutex<Ship>> {
        let mut ss = self.ships.lock().await;
        if let Some(s) = ss.get(&id) {
            return s.clone();
        }

        let s = Arc::new(Mutex::new(Ship::default()));
        ss.insert(id, s.clone());
        s
    }
}

#[cfg(unix)]
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    log::set_logger(&shared::logger::MasterLogger).expect("logger");
    log::set_max_level(log::LevelFilter::Debug);

    // let conf = Config::get();
    Config::create_dirs()?;
    let ships = Data::new(ActiveShips::default());

    let server = HttpServer::new(move || {
        App::new()
            .app_data(ships.clone())
            .app_data(web::PayloadConfig::new(100 * 1024 * 1024))
            .wrap(mw::Logger::new("%s %r %Ts"))
            .configure(config_app)
        // .wrap(mw::from_fn(bridge::headx))
    });

    if cfg!(debug_assertions) {
        // server.bind(("127.0.0.1", Config::PORT)).unwrap()
        server.bind(("0.0.0.0", Config::PORT)).unwrap()
    } else {
        use std::os::unix::fs::PermissionsExt;
        const PATH: &str = "/usr/share/nginx/socks/alzahra.sock";
        let server = server.bind_uds(PATH).unwrap();
        let perms = std::fs::Permissions::from_mode(0o777);
        std::fs::set_permissions(PATH, perms)?;
        server
    }
    .run()
    .await
}

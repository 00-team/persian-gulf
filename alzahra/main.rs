use std::collections::HashMap;
use tokio::sync::Mutex;
use actix_web::{
    App, HttpServer, middleware as mw,
    web::{ServiceConfig, scope},
};

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

struct Ship {
}

struct ActiveChannels {
    ships: HashMap<UniqueId, Mutex<Ship>>
}


#[cfg(unix)]
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    use crate::config::Config;

    log::set_logger(&shared::logger::MasterLogger).expect("logger");
    log::set_max_level(log::LevelFilter::Debug);

    // let conf = Config::get();
    Config::create_dirs()?;

    let server = HttpServer::new(move || {
        App::new().wrap(mw::Logger::new("%s %r %Ts")).configure(config_app)
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

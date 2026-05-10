use std::{collections::HashMap, sync::{Arc, OnceLock}};

use rustls::{ClientConfig, RootCertStore};

mod tom {
    use std::path::PathBuf;

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub struct ConfigToml {
        pub proxy: String,
        pub script_ids: Vec<(String, String)>,
        pub alzahra: Vec<String>,
        pub users: Vec<UserPass>,
    }

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub struct UserPass {
        pub user: String,
        pub pass: String,
    }

    impl ConfigToml {
        pub fn def() -> Self {
            Self {
                alzahra: vec!["http://94.183.183.223:9920".to_string()],
                proxy: "127.0.0.1:6007".to_string(),
                script_ids: vec![(
                    "your google script ids".to_string(),
                    "this scripts auth token".to_string(),
                )],
                users: vec![UserPass {
                    user: "user".to_string(),
                    pass: "pass".to_string(),
                }],
            }
        }
    }

    fn path() -> PathBuf {
        const DEFAULT: &str = "efg.toml";

        let mut args = std::env::args();
        let path = loop {
            let Some(arg) = args.next() else { break None };
            if arg == "-c" || arg == "--config" {
                break args.next();
            }
        }
        .unwrap_or(String::from(DEFAULT));

        PathBuf::from(path)
    }

    pub fn get() -> ConfigToml {
        let path = path();
        log::info!("reading toml config at: {path:?}");
        let data = match std::fs::read_to_string(&path) {
            Ok(v) => v,
            Err(_) => {
                let c = toml::to_string(&ConfigToml::def()).unwrap();
                std::fs::write("efg.example.toml", c.as_bytes()).unwrap();

                log::error!("move efg.example.toml to efg.toml and update it.");
                panic!("");
            }
        };

        match toml::from_str(&data) {
            Ok(v) => v,
            Err(e) => panic!("invalid toml config file: {path:?}\n{e:#?}"),
        }
    }
}

#[derive(Debug)]
/// `EverGreen` Config
pub struct Config {
    pub b64: base64::engine::GeneralPurpose,
    pub socks_bind: String,
    pub script_ids: Vec<(String, String)>,
    pub alzahra: Vec<String>,
    pub tls: Arc<ClientConfig>,
    // pub users: HashMap<String, String>,
}

impl Config {
    #![allow(clippy::inconsistent_digit_grouping)]

    // pub fn create_dirs() -> std::io::Result<()> {
    //     Ok(())
    // }

    fn init() -> Self {
        let ct = tom::get();
        let mut users = HashMap::with_capacity(ct.users.len());
        for up in ct.users {
            users.insert(up.user, up.pass);
        }

        Self {
            // users,
            socks_bind: ct.proxy,
            script_ids: ct.script_ids,
            alzahra: ct.alzahra,
            b64: base64::engine::GeneralPurpose::new(
                &base64::alphabet::STANDARD,
                base64::engine::GeneralPurposeConfig::new()
                    .with_encode_padding(false)
                    .with_decode_padding_mode(
                        base64::engine::DecodePaddingMode::Indifferent,
                    ),
            ),
            tls: Arc::new(build_tls_config()),
        }
    }

    pub fn get() -> &'static Self {
        static STATE: OnceLock<Config> = OnceLock::new();
        STATE.get_or_init(Self::init)
    }
}

fn build_tls_config() -> ClientConfig {
    let mut root_store = RootCertStore::empty();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install tls");

    // assert!(self.verify_ssl);
    // if self.verify_ssl {
    // Add platform-native root certificates (best for most users)
    // let native_certs =
    //     rustls_native_certs::load_native_certs().map_err(|e| {
    //         anyhow::anyhow!("failed to load native certs: {}", e)
    //     })?;
    // for cert in native_certs {
    //     root_store.add(cert)?;
    // }
    if root_store.is_empty() {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

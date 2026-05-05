use std::sync::OnceLock;

mod tom {
    use std::path::PathBuf;

    #[derive(Debug, serde::Deserialize)]
    pub struct ConfigToml {}

    fn path() -> PathBuf {
        const DEFAULT: &str = "config.toml";

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
            Err(e) => panic!("could not read config at: {path:?}\n{e:#?}"),
        };

        match toml::from_str(&data) {
            Ok(v) => v,
            Err(e) => panic!("invalid toml config file: {path:?}\n{e:#?}"),
        }
    }
}

#[derive(Debug)]
/// `Al-Zahra` Config
pub struct Config {}

impl Config {
    #![allow(clippy::inconsistent_digit_grouping)]

    // pub const API_VERSION: &str = "0.1.0";
    pub const PORT: u16 = 7707;

    pub fn create_dirs() -> std::io::Result<()> {
        Ok(())
    }

    fn init() -> Self {
        let ct = tom::get();

        Self {}
    }

    pub fn get() -> &'static Self {
        static STATE: OnceLock<Config> = OnceLock::new();
        STATE.get_or_init(Self::init)
    }
}

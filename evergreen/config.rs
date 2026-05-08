use std::sync::OnceLock;

mod tom {
    use std::path::PathBuf;

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub struct ConfigToml {
        pub proxy: String,
        pub script_ids: Vec<(String, String)>,
        pub alzahra: String,
    }

    impl ConfigToml {
        pub fn def() -> Self {
            Self {
                alzahra: "http://94.183.183.223:9920".to_string(),
                proxy: "127.0.0.1:6007".to_string(),
                script_ids: vec![(
                    "your google script ids".to_string(),
                    "this scripts auth token".to_string(),
                )],
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

                log::error!(
                    "move efg.example.toml to efg.toml and update it."
                );
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
    pub alzahra: String,
}

impl Config {
    #![allow(clippy::inconsistent_digit_grouping)]

    pub fn create_dirs() -> std::io::Result<()> {
        Ok(())
    }

    fn init() -> Self {
        let ct = tom::get();

        Self {
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
        }
    }

    pub fn get() -> &'static Self {
        static STATE: OnceLock<Config> = OnceLock::new();
        STATE.get_or_init(Self::init)
    }
}

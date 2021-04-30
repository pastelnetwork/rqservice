use config::{ConfigError, Config, File};
use serde_derive::Deserialize;

use crate::cmd;

#[derive(Debug, Deserialize)]
pub struct ConfigFile {
    pub global_string: String,
    pub global_integer: u64,
    pub service: ServiceConfig,
    // peers: Option<Vec<PeerConfig>>,
}

#[derive(Debug, Deserialize)]
pub struct ServiceConfig {
    pub ip: String,
    pub port: u64,
}

pub struct Settings {
    pub cfg: ConfigFile,
    pub cmd: cmd::ApplicationArguments,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {

        let cmd = cmd::ApplicationArguments::new();

        let mut cfg = Config::new();
        cfg.merge(File::with_name(&cmd.config_file))?;
        let mut c : ConfigFile = cfg.try_into()?;

        if cmd.service_listen_addr.is_some() {
            let i = cmd.service_listen_addr.clone();
            c.service.ip = i.unwrap();
        }

        let s = Settings{cfg: c, cmd};
        Ok(s)
    }
}
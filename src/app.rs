// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use clap::{Arg, App, SubCommand, ArgMatches, Error};
use config::{ConfigError, Config, File};
use std::collections::HashMap;
use std::env;
use dirs;

const NIX_PASTELD_PATH: &str = ".pastel";
const MAC_PASTELD_PATH: &str = "Library/Application Support/Pastel";
const WIN_PASTELD_PATH: &str = "AppData\\Roaming\\Pastel";
const DEFAULT_CONFIG_FILE: &str = "rqservice";

pub struct ServiceSettings {
    pub grpc_service: String,
    pub pastel_path: String,
    pub config_path: String
}

impl ServiceSettings {

    pub fn new() -> Result<Self, ConfigError> {

        let mut pastel_path = String::new();
        let mut config_path = String::new();

        match dirs::home_dir() {
            Some(path) => {
                if env::consts::OS == "linux" {
                    pastel_path = format!("{}/{}", path.display(), NIX_PASTELD_PATH);
                    config_path = format!("{}/{}", pastel_path, DEFAULT_CONFIG_FILE);
                } else if env::consts::OS == "macos" {
                    pastel_path = format!("{}/{}", path.display(), MAC_PASTELD_PATH);
                    config_path = format!("{}/{}", pastel_path, DEFAULT_CONFIG_FILE);
                } else if env::consts::OS == "windows" {
                    pastel_path = format!("{}\\{}", path.display(), WIN_PASTELD_PATH);
                    config_path = format!("{}\\{}", pastel_path, DEFAULT_CONFIG_FILE);
                } else {
                    panic!("Unsupported system!");
                }
            },
            None => panic!("Unsupported system!")
        }

        let app = App::new("rqservice")
            .version("v0.1.0")
            .author("Pastel Network <pastel.network>")
            .about("RaptorQ Service")
            .arg(Arg::with_name("config")
                .short("c")
                .long("config-file")
                .value_name("FILE")
                .help(format!("Set path to the config file. (default: {})", config_path).as_str())
                .takes_value(true))
            .arg(Arg::with_name("grpc-service")
                .short("s")
                .long("grpc-service")
                .value_name("IP:PORT")
                .help("Set IP address and PORT for gRPC server to listen on. (default: 127.0.0.1:50051)")
                .takes_value(true))
            .get_matches();

        let config_file = app.value_of("config").unwrap_or(&config_path);

        let mut cfg = Config::default();
        if let Err(E) = cfg.merge(File::with_name(&config_file)) {
            println!("Cannot read config file {} - {}", config_file, E);
        }

        let grpc_service = ServiceSettings::find_setting(app, cfg, "grpc-service", "127.0.0.1:50051".to_string(), true);

        Ok(ServiceSettings{grpc_service, pastel_path, config_path})
    }

    fn find_setting( app: ArgMatches, cfg: Config, name: &str, default: String, must: bool ) -> String {
        let param: String;
        match app.value_of(&name) {
            Some(v) => param = v.to_string(),
            None => {
                match cfg.get::<String>(&name) {
                    Ok(v) => param = v,
                    Err(err) => {
                        if must {
                            panic!(format!("Parameter {} not found", &name))
                        } else {
                            param = default;
                        }
                    }
                }
            }
        }
        param
    }
}
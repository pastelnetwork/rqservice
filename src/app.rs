// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use clap::Parser;
use clap_derive::Parser;
use std::env;
use config::ConfigError;

const NIX_PASTELD_PATH: &str = ".pastel";
const MAC_PASTELD_PATH: &str = "Library/Application Support/Pastel";
const WIN_PASTELD_PATH: &str = "AppData\\Roaming\\Pastel";
const DEFAULT_CONFIG_FILE: &str = "rqservice";

#[derive(Debug, Default, Clone)]
pub struct ServiceSettings {
    pub grpc_service: String,
    pub symbol_size: u16,
    pub redundancy_factor: u8,
    pub pastel_path: String,
    pub config_path: String
}

#[derive(Parser, Debug)]
struct CmdArgs {
    #[clap(short, long, value_name = "FILE", default_value = DEFAULT_CONFIG_FILE)]
    config: String,
    #[clap(short, long, value_name = "IP:PORT", default_value = "127.0.0.1:50051")]
    grpc_service: String,
}

fn cmd_args_new() -> CmdArgs {
    CmdArgs::parse()
}

impl ServiceSettings {
    pub fn new() -> Result<Self, ConfigError> {
        let pastel_path;
        let config_path;

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

        let cmd_args = cmd_args_new();

        // Assuming default values for symbol_size and redundancy_factor
        Ok(ServiceSettings {
            grpc_service: cmd_args.grpc_service,
            symbol_size: 50000, // You might want to retrieve this value from a config file or define it as a constant
            redundancy_factor: 12, // Same as above
            pastel_path,
            config_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_service_settings_linux() {
        if env::consts::OS != "linux" {
            return;
        }
        let settings = ServiceSettings::new().unwrap();
        assert_eq!(settings.grpc_service, "127.0.0.1:50051");
        assert_eq!(settings.symbol_size, 50000);
        assert_eq!(settings.redundancy_factor, 12);
        assert!(settings.pastel_path.ends_with(".pastel"));
        assert!(settings.config_path.ends_with("rqservice"));
    }

    #[test]
    fn test_service_settings_macos() {
        if env::consts::OS != "macos" {
            return;
        }
        let settings = ServiceSettings::new().unwrap();
        assert_eq!(settings.grpc_service, "127.0.0.1:50051");
        assert_eq!(settings.symbol_size, 50000);
        assert_eq!(settings.redundancy_factor, 12);
        assert!(settings.pastel_path.ends_with("Library/Application Support/Pastel"));
        assert!(settings.config_path.ends_with("rqservice"));
    }

    #[test]
    fn test_service_settings_windows() {
        if env::consts::OS != "windows" {
            return;
        }
        let settings = ServiceSettings::new().unwrap();
        assert_eq!(settings.grpc_service, "127.0.0.1:50051");
        assert_eq!(settings.symbol_size, 50000);
        assert_eq!(settings.redundancy_factor, 12);
        assert!(settings.pastel_path.ends_with("AppData\\Roaming\\Pastel"));
        assert!(settings.config_path.ends_with("rqservice"));
    }

    #[test]
    #[should_panic]
    fn test_service_settings_unsupported() {
        env::set_var("OS", "unsupported");
        ServiceSettings::new().unwrap();
    }
}

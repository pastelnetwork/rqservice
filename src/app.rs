// Copyright (c) 2021-2023 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use clap::Parser;
use clap_derive::Parser;
use std::env;
use config::ConfigError;
use dirs;

fn nix_pasteld_path() -> String {
    let home_dir = dirs::home_dir().expect("Could not find home directory");
    home_dir.join(".pastel").to_str().expect("Could not convert path to string").to_owned()
}

const MAC_PASTELD_PATH: &str = "Library/Application Support/Pastel";
const WIN_PASTELD_PATH: &str = "AppData\\Roaming\\Pastel";
const DEFAULT_CONFIG_FILE: &str = "rqconfig";

fn get_os_type() -> String {
    env::var("TEST_OS").unwrap_or_else(|_| env::consts::OS.to_string())
}

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
        let unix_pasteld_path = nix_pasteld_path();
        println!("OS Type: {}", get_os_type()); // Debug print
        match dirs::home_dir() {
            Some(path) => {
                if get_os_type() == "linux" {
                    log::info!("Using Linux system");
                    pastel_path = format!("{}/{}", path.display(), &unix_pasteld_path);
                    config_path = format!("{}/{}", pastel_path, DEFAULT_CONFIG_FILE);
                } else if get_os_type() == "macos" {
                    log::info!("Using MacOS system");
                    pastel_path = format!("{}/{}", path.display(), MAC_PASTELD_PATH);
                    config_path = format!("{}/{}", pastel_path, DEFAULT_CONFIG_FILE);
                } else if get_os_type() == "windows" {
                    log::info!("Using Windows system");
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

    fn set_up_os(os: &str) {
        env::set_var("TEST_OS", os);
    }

    fn tear_down_os() {
        env::remove_var("TEST_OS");
    }

    #[test]
    fn test_service_settings_linux() {
        set_up_os("linux");
        let settings = ServiceSettings::new().unwrap();
        tear_down_os(); // Ensure this is called even if the test panics

        assert_eq!(settings.grpc_service, "127.0.0.1:50051");
        assert_eq!(settings.symbol_size, 50000);
        assert_eq!(settings.redundancy_factor, 12);
        assert!(settings.pastel_path.ends_with(".pastel"));
        assert!(settings.config_path.ends_with("rqconfig"));
    }

    #[test]
    fn test_service_settings_macos() {
        set_up_os("macos"); // Set up test environment for MacOS
        let settings = ServiceSettings::new().unwrap();
        tear_down_os(); // Reset environment after test
    
        assert_eq!(settings.grpc_service, "127.0.0.1:50051");
        assert_eq!(settings.symbol_size, 50000);
        assert_eq!(settings.redundancy_factor, 12);
        assert!(settings.pastel_path.ends_with("Library/Application Support/Pastel"));
        assert!(settings.config_path.contains("Library/Application Support/Pastel"));
    }
    
    #[test]
    fn test_service_settings_windows() {
        set_up_os("windows"); // Set up test environment for Windows
        let settings = ServiceSettings::new().unwrap();
        tear_down_os(); // Reset environment after test
    
        assert_eq!(settings.grpc_service, "127.0.0.1:50051");
        assert_eq!(settings.symbol_size, 50000);
        assert_eq!(settings.redundancy_factor, 12);
        assert!(settings.pastel_path.ends_with("AppData\\Roaming\\Pastel"));
        assert!(settings.config_path.contains("AppData\\Roaming\\Pastel"));
    }
    

    #[test]
    #[should_panic(expected = "Unsupported system")]
    fn test_service_settings_unsupported() {
        set_up_os("unsupported"); // Explicitly test for unsupported OS
        let _ = ServiceSettings::new();
        tear_down_os(); // Ensure cleanup even if the test panics
    }

}

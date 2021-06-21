// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use std::error::Error;
use flexi_logger::{Logger, FileSpec, WriteMode};
use std::net::Shutdown::Write;

// pub mod rq;
pub mod app;
pub mod rqserver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let settings = app::ServiceSettings::new()?;
    let _logger = Logger::try_with_str("info")?
        .log_to_file(
            FileSpec::default().suppress_timestamp()
                .directory(&settings.pastel_path)
                .basename("rqservice")
        )
        .append()
        .write_mode(WriteMode::Async)
        .start()?;

    log::info!("{}", &settings.grpc_service);
    // println!("{}", settings.grpc_service);

    rqserver::start_server(&settings).await?;

    //
    // match settings.cmd.subcommand {
    //     cmd::SubCommand::StartService(_opts) => {
    //         println!("Start the service on: {:?}:{:?}", settings.cfg.service.ip, settings.cfg.service.port);
    //     }
    //     cmd::SubCommand::Command(opts) => {
    //         println!("Run command: '{:?}'", opts.command);
    //     }
    // }

    Ok(())
}

pub mod rqservice;
pub mod app;

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let settings = app::ServiceSettings::new()?;

    println!("{}", settings.grpc_service);

    // rqservice::server::start_server(settings).await?;

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

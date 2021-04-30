pub mod cli;

use cli::cmd;
use cli::settings;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {

    let settings = settings::Settings::new()?;

    match settings.cmd.subcommand {
        cmd::SubCommand::StartService(_opts) => {
            println!("Start the service on: {:?}:{:?}", settings.cfg.service.ip, settings.cfg.service.port);
        }
        cmd::SubCommand::Command(opts) => {
            println!("Run command: '{:?}'", opts.command);
        }
    }

    Ok(())
}

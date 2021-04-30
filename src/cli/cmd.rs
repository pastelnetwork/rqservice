use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct ServiceOptions {
}

#[derive(Debug, StructOpt)]
pub struct ClientOptions {
    /// The full command and arguments for the server to execute
    pub command: Vec<String>,
}

#[derive(Debug, StructOpt)]
pub enum SubCommand {
    /// Start the remote command gRPC service
    #[structopt(name = "service")]
    StartService(ServiceOptions),
    /// Send a command to the gRPC service
    #[structopt(setting = structopt::clap::AppSettings::TrailingVarArg)]
    Command(ClientOptions),
}

#[derive(StructOpt, Debug)]
#[structopt(name = "rqservice")]
pub struct ApplicationArguments {
    #[structopt(flatten)]
    pub subcommand: SubCommand,

    /// Path to the config file.
    #[structopt(long, default_value = "./rqconfig.toml")]
    pub config_file: String,

    /// The address of the gPRC service.
    #[structopt(long)]
    pub service_listen_addr: Option<String>,
}

impl ApplicationArguments {
    pub fn new() -> ApplicationArguments {
        ApplicationArguments::from_args()
    }
}
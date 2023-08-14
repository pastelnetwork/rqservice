// Copyright (c) 2021-2023 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use flexi_logger::{Logger, FileSpec, WriteMode};

pub mod app;
pub mod rqserver;
pub mod rqprocessor;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

// pub const DB_PATH: &str = "/home/ubuntu/.pastel/testnet3/rq_symbols.sqlite";
pub const DB_PATH: &str = "/home/ubuntu/rqservice/test_files/rq_symbols.sqlite";

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

    // Initialize the database
    rqprocessor::initialize_database(DB_PATH).unwrap();

    let manager = SqliteConnectionManager::file(rqprocessor::DB_PATH);
    let pool = Pool::new(manager).expect("Failed to create pool.");

    rqserver::start_server(&settings, &pool).await?;

    Ok(())
}

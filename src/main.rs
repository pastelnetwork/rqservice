// Copyright (c) 2021-2023 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use flexi_logger::{Logger, FileSpec, WriteMode};
use std::sync::Arc;

pub mod app;
pub mod rqserver;
pub mod rqprocessor;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::str::FromStr;
use cron::Schedule;
use std::thread;
use chrono::{Utc, Duration as ChronoDuration};
use rqprocessor::RaptorQProcessor;

pub const DB_PATH: &str = "/home/ubuntu/.pastel/testnet3/rq_symbols.sqlite";
// pub const DB_PATH: &str = "/home/ubuntu/rqservice/test_files/rq_symbols.sqlite";

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
        .log_to_stdout() // Add this line to also log to stdout
        .start()?;

    log::info!("Now starting RQ-Service...");
    // Initialize the database

    log::info!("Initializing RQ-Service database...");
    rqprocessor::initialize_database(DB_PATH).unwrap();
    log::info!("Creating database pool...");
    let manager = SqliteConnectionManager::file(rqprocessor::DB_PATH);
    let pool = Arc::new(Pool::new(manager).expect("Failed to create pool."));

    log::info!("Creating RaptorQ Processor instance...");
    // Create the RaptorQProcessor instance
    let rq_processor = RaptorQProcessor::new(DB_PATH)?;

    // Clone the Arc for the spawned thread
    let pool_clone = Arc::clone(&pool);

    // Spawn a background thread to run the maintenance task
    thread::spawn(move || {
        let cron_expression = "0 0 3 * * *"; // Run daily at 3:00 AM
        let schedule = Schedule::from_str(cron_expression).unwrap();

        // Iterate over the schedule to execute the task
        for datetime in schedule.upcoming(Utc) {
            let now = Utc::now();
            let time_until_next_job = datetime.signed_duration_since(now);
            if time_until_next_job > ChronoDuration::seconds(0) {
                thread::sleep(time_until_next_job.to_std().unwrap());
            }

            // Get a connection from the pool
            if let Ok(conn) = pool_clone.get() {
                // Execute the maintenance function
                if let Err(err) = rq_processor.db_maintenance_func(&conn) {
                    log::error!("Error during database maintenance: {:?}", err);
                }
            }
        }
    });
    log::info!("Starting RQ-Service server...");
    rqserver::start_server(&settings, &pool).await?;
    log::info!("RQ-Service server started, listening for requests...");

    Ok(())
}

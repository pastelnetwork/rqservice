// Copyright (c) 2021-2023 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};
use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use std::{fmt, fs, io};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use chrono::{Duration as ChronoDuration, Utc};
use serde_derive::{Deserialize, Serialize};
use rusqlite::{Connection, params, ToSql};
use rayon::prelude::*;
use rand::Rng;
use lazy_static::lazy_static;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use toml;
use itertools::Itertools;

lazy_static! {
    static ref WRITE_LOCK: Mutex<()> = Mutex::new(());
}

const MAX_DAYS_OLD_BEFORE_RQ_SYMBOL_FILES_ARE_PURGED: i64 = 2; // Adjust as needed
pub const NUM_WORKERS: usize = 5;
pub const ORIGINAL_FILE_LOAD_CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10 MB
pub static DB_PATH: &once_cell::sync::Lazy<String> = &crate::DB_PATH;
pub static RQ_FILES_PATH: &once_cell::sync::Lazy<String> = &crate::RQ_FILES_PATH;
pub static RQ_CONFIG_PATH: &once_cell::sync::Lazy<String> = &crate::RQ_CONFIG_PATH;



#[derive(Deserialize)]
struct RqConfig {
    #[allow(dead_code)]
    #[serde(rename = "grpc-service")]
    grpc_service: String,
    #[serde(rename = "symbol-size")]
    symbol_size: u16,
    #[serde(rename = "redundancy-factor")]
    redundancy_factor: u8,
}

fn read_config() -> Result<RqConfig, Box<dyn std::error::Error>> {
    let path_str = &**RQ_CONFIG_PATH; // Dereferencing to get the underlying String
    let contents = fs::read_to_string(path_str)?; // Reading the file
    let config: RqConfig = toml::from_str(&contents)?;
    Ok(config)
}


impl RqProcessorError {
    pub fn new(func: &str, msg: &str, prev_msg: String) -> RqProcessorError {
        RqProcessorError {
            func: func.to_string(),
            msg: msg.to_string(),
            prev_msg
        }
    }
    pub fn new_file_err(func: &str, msg: &str, path: &Path, prev_msg: String) -> RqProcessorError {
        RqProcessorError {
            func: func.to_string(),
            msg: format!("{} [path: {:?}]", msg, path),
            prev_msg
        }
    }
    pub fn get_message(&self) -> &str {
        &self.msg
    }
}

impl std::error::Error for RqProcessorError {}
impl fmt::Display for RqProcessorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "In [{}], Error [{}] (internal error - {})", self.func, self.msg, self.prev_msg)
    }
}

#[derive(Debug, Clone)]
pub struct RqProcessorError {
    func: String,
    msg: String,
    prev_msg: String
}

fn get_current_timestamp() -> i64 {
    Utc::now().timestamp()
}

enum WriteOperation {
    OriginalFile((String, String, f64, u32, String, String, Vec<u8>)),
    Symbol((String, String, String, Vec<u8>, i64)),
    UpdateOriginalFileHash((String, String)),
    Terminate, // New termination signal
}

pub fn initialize_database(db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    RaptorQProcessor::initialize_db(db_path)?;
    Ok(())
}

pub struct RaptorQProcessor {
    symbol_size: u16,
    redundancy_factor: u8,
}

impl RaptorQProcessor {

    pub fn new(db_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::initialize_db(db_path)?; // Initialize the database
        let config = read_config()?;
        Ok(RaptorQProcessor {
            symbol_size: config.symbol_size,
            redundancy_factor: config.redundancy_factor,
        })
    }

    pub fn compute_original_file_hash(&self, input: &Path) -> Result<String, RqProcessorError> {
        let mut file = File::open(input)
            .map_err(|err| RqProcessorError::new_file_err("compute_original_file_hash", "Cannot open file", input, err.to_string()))?;
        let mut hasher = Sha3_256::new();
        const CHUNK_SIZE: usize = 512 * 1024; // 512 KB
        let mut buffer = vec![0u8; CHUNK_SIZE]; // Buffer to read chunks of the file
        // Read file in chunks and update the hash
        loop {
            let n = file.read(&mut buffer)
                .map_err(|err| RqProcessorError::new_file_err("compute_original_file_hash", "Cannot read file", input, err.to_string()))?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[0..n]);
        }
        let hash_result = hasher.finalize();
        Ok(bs58::encode(&hash_result).into_string())
    }
    
    pub fn initialize_db(path: &str) -> Result<Connection, rusqlite::Error> {
        log::info!("Initializing database at path: {}", path);
        let conn = Connection::open(path)?;
        let desired_page_size = 65536;
        log::debug!("Querying current page size");
        let current_page_size: i32 = conn.query_row("PRAGMA page_size;", params![], |row| row.get(0))?;
        if current_page_size != desired_page_size {
            log::debug!("Setting page size to {}", desired_page_size);
            conn.execute_batch(&format!("PRAGMA page_size = {};", desired_page_size))?;
            conn.execute_batch("VACUUM;")?;
        }
        macro_rules! set_pragma_if_different {
            ($pragma:expr, $value:expr) => {
                log::debug!("Checking PRAGMA {}: current value", $pragma);
                let current_value: String = conn.query_row(&format!("PRAGMA {};", $pragma), params![], |row| {
                    let value_as_i32: Result<i32, _> = row.get(0);
                    match value_as_i32 {
                        Ok(v) => Ok(v.to_string()),
                        Err(_) => row.get(0)
                    }
                })?;
                if current_value != $value {
                    log::debug!("Setting PRAGMA {} = {}", $pragma, $value);
                    conn.execute_batch(&format!("PRAGMA {} = {};", $pragma, $value))?;
                }
            };
        }
        set_pragma_if_different!("journal_mode", "WAL");
        set_pragma_if_different!("wal_autocheckpoint", "2000");
        set_pragma_if_different!("synchronous", "NORMAL");
        set_pragma_if_different!("cache_size", "-262144");
        set_pragma_if_different!("busy_timeout", "2000");
        log::debug!("Creating tables");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rq_symbols (
                original_file_sha3_256_hash TEXT,
                task_id TEXT,
                rq_symbol_file_sha3_256_hash TEXT PRIMARY KEY,
                rq_symbol_file_data BLOB,
                utc_datetime_symbol_file_created INT
            )",
            params![],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS original_files (
                original_file_sha3_256_hash TEXT PRIMARY KEY,
                original_file_path TEXT,
                original_file_size_in_mb REAL,
                files_number INT,
                encoder_parameters BLOB,
                block_hash TEXT,
                pastel_id TEXT
            )",
            params![],
        )?;
        log::debug!("Database initialization complete");
        Ok(conn)
    }

    pub fn db_maintenance_func(&self, conn: &Connection) -> Result<(), rusqlite::Error> {
        // Calculate the threshold date
        let threshold_date = Utc::now() - ChronoDuration::days(MAX_DAYS_OLD_BEFORE_RQ_SYMBOL_FILES_ARE_PURGED);

        // Convert the threshold date to a suitable format for your database, e.g., a timestamp or string
        let threshold_timestamp = threshold_date.timestamp(); // If using a timestamp in the database

        // Find original file hashes with outdated symbols
        let mut stmt = conn.prepare(
            "SELECT DISTINCT original_file_sha3_256_hash FROM rq_symbols WHERE utc_datetime_symbol_file_created < ?1",
        )?;
        let original_hashes_to_check: Vec<String> = stmt.query_map(params![threshold_timestamp], |row| row.get(0))?.collect::<Result<_, _>>()?;

        for original_hash in &original_hashes_to_check {
            // Delete outdated symbols
            conn.execute(
                "DELETE FROM rq_symbols WHERE original_file_sha3_256_hash = ?1 AND utc_datetime_symbol_file_created < ?2",
                params![original_hash, threshold_timestamp],
            )?;
            log::info!("Deleted outdated symbols for original file hash: {}", original_hash);

            // Check if there are any symbols left for this original file hash
            let symbols_left: i32 = conn.query_row(
                "SELECT COUNT(*) FROM rq_symbols WHERE original_file_sha3_256_hash = ?1",
                params![original_hash],
                |row| row.get(0),
            )?;

            // If no symbols left, delete entry from original_files table
            if symbols_left == 0 {
                conn.execute(
                    "DELETE FROM original_files WHERE original_file_sha3_256_hash = ?1",
                    params![original_hash],
                )?;
                log::info!("Deleted original file entry for hash: {}", original_hash);
            }
        }

        Ok(())
    }

    fn insert_symbols_batch(
        conn: &mut r2d2::PooledConnection<SqliteConnectionManager>,
        symbols: &[(String, String, String, Vec<u8>, i64)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO rq_symbols (original_file_sha3_256_hash, task_id, rq_symbol_file_sha3_256_hash, rq_symbol_file_data, utc_datetime_symbol_file_created) VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for (original_file_hash, task_id, symbol_hash, symbol_data, timestamp) in symbols.iter() {
                let rows_affected = stmt.execute(params![original_file_hash, task_id, symbol_hash, symbol_data, timestamp])?;
                if rows_affected == 0 {
                    log::info!("Symbol with hash {} already exists in the database. Skipping insertion.", symbol_hash);
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
    
    fn insert_original_file(tx: &rusqlite::Transaction, original_file_hash: &str, original_file_path: &str, original_file_size_in_mb: f64, files_number: u32, block_hash: &str, pastel_id: &str, encoder_parameters: &Vec<u8>) -> Result<(), rusqlite::Error> {
        log::info!("Inserting metadata for original file with hash: {}", original_file_hash);
        tx.execute(
            "INSERT OR REPLACE INTO original_files (original_file_sha3_256_hash, original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![original_file_hash, original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters],
        )?;
        log::info!("Inserted original file with hash: {}", original_file_hash);
        Ok(())
    }

    fn insert_symbols_with_retry(
        conn: &mut r2d2::PooledConnection<SqliteConnectionManager>,
        symbol_batch: &[(String, String, String, Vec<u8>, i64)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY: Duration = Duration::from_millis(50);
        let mut retries = 0;
        loop {
            match RaptorQProcessor::insert_symbols_batch(conn, symbol_batch) {
                Ok(_) => {
                    log::info!("Symbols batch inserted successfully, containing {} symbol files.", symbol_batch.len());
                    break;
                }
                Err(err) if retries < MAX_RETRIES => {
                    log::warn!("Retrying symbols batch insertion. Retry count: {}. Error: {}", retries, err);
                    retries += 1;
                    let jitter = rand::thread_rng().gen_range(0..10);
                    std::thread::sleep(RETRY_DELAY + Duration::from_millis(jitter));
                }
                Err(err) => {
                    log::error!("Failed to insert symbols batch. Error: {}", err);
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }
    
    fn insert_original_file_metadata(
        original_file_hash: &String,
        path: &String,
        block_hash: &String,
        pastel_id: &String,
        enc_config: &Vec<u8>,
        file_size: f64,
        files_number: u32,
        tx_queue: &crossbeam::channel::Sender<WriteOperation>,
    ) -> Result<(), RqProcessorError> {
        let original_file_metadata: (String, String, f64, u32, String, String, Vec<u8>) = (
            original_file_hash.clone(),
            path.clone(),
            file_size,
            files_number,
            block_hash.clone(),
            pastel_id.clone(),
            enc_config.clone(),
        );
        tx_queue
            .send(WriteOperation::OriginalFile(original_file_metadata))
            .unwrap();
        Ok(())
    }

    fn insert_worker_func(
        rx_queue: crossbeam::channel::Receiver<WriteOperation>,
        mut conn: r2d2::PooledConnection<SqliteConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        const MAX_RETRIES: u32 = 5;
        const BATCH_SIZE: usize = 25000;
        const RETRY_DELAY: Duration = Duration::from_millis(50);
        let mut symbol_batch: Vec<(String, String, String, Vec<u8>, i64)> = Vec::with_capacity(BATCH_SIZE);
        log::info!("Insert worker started with settings: MAX_RETRIES = {}, BATCH_SIZE = {}, RETRY_DELAY = {:?}", MAX_RETRIES, BATCH_SIZE, RETRY_DELAY);
        for write_op in rx_queue.iter() {
            match write_op {
                WriteOperation::Symbol(symbol_data) => {
                    symbol_batch.push(symbol_data);
                    if symbol_batch.len() == BATCH_SIZE {
                        let mut retries = 0;
                        loop {
                            log::info!("Attempting to acquire write lock now...");
                            let _guard = WRITE_LOCK.lock().map_err(|err| {
                                RqProcessorError::new("insert_worker_func", "Failed to acquire write lock", err.to_string())
                            })?;
                            match RaptorQProcessor::insert_symbols_with_retry(&mut conn, &symbol_batch) {
                                Ok(_) => {
                                    symbol_batch.clear();
                                    break;
                                },
                                Err(_) if retries < MAX_RETRIES => {
                                    retries += 1;
                                    let jitter = rand::thread_rng().gen_range(0..10);
                                    std::thread::sleep(RETRY_DELAY + Duration::from_millis(jitter));
                                },
                                Err(err) => return Err(err.into()), // Convert the error into a boxed error
                            }
                        }
                    }
                },
                WriteOperation::OriginalFile((original_file_hash, original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters)) => {
                    let mut retries = 0;
                    loop {
                        log::info!("Attempting to acquire write lock now...");
                        let _guard = WRITE_LOCK.lock().map_err(|err| {
                            RqProcessorError::new("insert_worker_func", "Failed to acquire write lock", err.to_string())
                        })?;
                        match conn.transaction() {
                            Ok(tx) => {
                                RaptorQProcessor::insert_original_file(&tx, &original_file_hash, &original_file_path, original_file_size_in_mb, files_number, &block_hash, &pastel_id, &encoder_parameters)?;
                                tx.commit()?;
                                break;
                            },
                            Err(_) if retries < MAX_RETRIES => {
                                retries += 1;
                                let jitter = rand::thread_rng().gen_range(0..10);
                                std::thread::sleep(RETRY_DELAY + Duration::from_millis(jitter));
                            },
                            Err(err) => return Err(err.into()), // Convert the error into a boxed error
                        }
                    }
                },
                WriteOperation::UpdateOriginalFileHash((task_id, original_file_hash)) => {
                    let mut retries = 0;
                    loop {
                        log::info!("Attempting to acquire write lock now...");
                        let _guard = WRITE_LOCK.lock().map_err(|err| {
                            RqProcessorError::new("insert_worker_func", "Failed to acquire write lock", err.to_string())
                        })?;
                        match conn.prepare("UPDATE rq_symbols SET original_file_sha3_256_hash = ?1 WHERE task_id = ?2") {
                            Ok(mut update_stmt) => {
                                match update_stmt.execute(params![original_file_hash, task_id]) {
                                    Ok(_) => break,
                                    Err(_) if retries < MAX_RETRIES => {
                                        retries += 1;
                                        let jitter = rand::thread_rng().gen_range(0..10);
                                        std::thread::sleep(RETRY_DELAY + Duration::from_millis(jitter));
                                    },
                                    Err(err) => return Err(err.into()), // Convert the error into a boxed error
                                }
                            },
                            Err(_) if retries < MAX_RETRIES => {
                                retries += 1;
                                let jitter = rand::thread_rng().gen_range(0..10);
                                std::thread::sleep(RETRY_DELAY + Duration::from_millis(jitter));
                            },
                            Err(err) => return Err(err.into()), // Convert the error into a boxed error
                        }
                    }
                },
                WriteOperation::Terminate => {
                    break; // Break out of the loop when the termination signal is received
                },
            }
        }
        // Insert any remaining symbols in the batch with retry logic
        if !symbol_batch.is_empty() {
            log::info!("Inserting remaining symbols, count: {}", symbol_batch.len());
            let mut retries = 0;
            loop {
                log::info!("Attempting to acquire write lock now...");
                let _guard = WRITE_LOCK.lock().map_err(|err| {
                    RqProcessorError::new("insert_worker_func", "Failed to acquire write lock", err.to_string())
                })?;
                match RaptorQProcessor::insert_symbols_with_retry(&mut conn, &symbol_batch) {
                    Ok(_) => break,
                    Err(_) if retries < MAX_RETRIES => {
                        retries += 1;
                        let jitter = rand::thread_rng().gen_range(0..10);
                        std::thread::sleep(RETRY_DELAY + Duration::from_millis(jitter));
                    },
                    Err(err) => return Err(err.into()), // Convert the error into a boxed error
                }
            }
        }
        log::info!("Insert worker function completed successfully.");
        // Now do manual WAL checkpoint full:
        conn.execute_batch("PRAGMA wal_checkpoint(FULL);")?;
        Ok(())
    }

    fn original_file_exists(conn: &Connection, original_file_hash: &str) -> Result<bool, rusqlite::Error> {
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM original_files WHERE original_file_sha3_256_hash = ?1",
            params![original_file_hash],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    fn check_if_rq_symbol_files_are_already_in_db(
        pool: &r2d2::Pool<SqliteConnectionManager>,
        path_to_rq_symbol_files_for_file: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut missing_hashes = Vec::new();
        // Iterate over the symbol files in the given directory
        for entry in std::fs::read_dir(path_to_rq_symbol_files_for_file)? {
            let entry = entry?;
            let symbol_path = entry.path();
            // Extract the hash from the file name
            let symbol_hash = symbol_path.file_name()
                .and_then(|name| name.to_str())
                .ok_or("Invalid symbol hash")?
                .to_string();
            // Check if the symbol already exists in the database
            let conn = pool.get().expect("Failed to get connection from pool.");
            let symbol_exists: i64 = conn.query_row(
                "SELECT COUNT(*) FROM rq_symbols WHERE symbol_hash = ?1", 
                params![symbol_hash], 
                |row| row.get(0)
            ).unwrap_or(0);
            // If the symbol doesn't exist, add it to the list of missing hashes
            if symbol_exists == 0 {
                missing_hashes.push(symbol_hash);
            }
        }
        Ok(missing_hashes)
    }

    fn load_rq_symbol_files_into_db_from_folder(
        pool: &r2d2::Pool<SqliteConnectionManager>,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("Loading RQ symbol files from folder: {}", path);
        let task_id = path.rsplit("/").nth(1).ok_or("Invalid path format")?;
        log::info!("Task ID: {}", task_id);
        let symbols_path = path.to_string();
        let (tx_queue, rx_queue) = crossbeam::channel::unbounded();
        // Launch worker threads
        let workers: Vec<_> = (0..NUM_WORKERS)
            .map(|_| {
                let rx_queue = rx_queue.clone();
                let pool = pool.clone();
                std::thread::spawn(move || {
                    let conn: r2d2::PooledConnection<SqliteConnectionManager> = pool.get().expect("Failed to get connection from pool.");
                    RaptorQProcessor::insert_worker_func(rx_queue, conn).expect("Insert worker failed");
                })
            })
            .collect();
        let conn = pool.get().expect("Failed to get connection from pool.");
        // Iterate through symbol files and send WriteOperations to worker threads
        for entry in std::fs::read_dir(symbols_path)? {
            let entry = entry?;
            let symbol_path = entry.path();
            let symbol_hash = symbol_path.file_name()
                .and_then(|name| name.to_str())
                .ok_or("Invalid symbol hash")?;
            // Check if the symbol already exists in the database for the specific task_id
            let symbol_exists: i64 = conn.query_row(
                "SELECT COUNT(*) FROM rq_symbols WHERE symbol_hash = ?1",
                params![symbol_hash],
                |row| row.get(0),
            ).unwrap_or(0);
            if symbol_exists > 0 {
                log::info!("Symbol with hash {} already exists in the database for task_id {}. Skipping insertion.", symbol_hash, task_id);
                continue;
            }
            let symbol_data = std::fs::read(symbol_path.clone())?;
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
            let original_file_hash = "";
            tx_queue.send(WriteOperation::Symbol((original_file_hash.to_string(), task_id.to_string(), symbol_hash.to_string(), symbol_data, timestamp))).unwrap();
        }
        // Send termination signal to worker threads
        for _ in 0..NUM_WORKERS {
            tx_queue.send(WriteOperation::Terminate).unwrap();
        }
        // Wait for worker threads to complete
        for worker in workers {
            worker.join().expect("Worker thread panicked");
        }
        Ok(())
    }

    pub fn create_metadata(
        &self,
        path: &String,
        block_hash: &String,
        pastel_id: &String,
        pool: &Pool<SqliteConnectionManager>,
    ) -> Result<(EncoderMetaData, String), RqProcessorError> {
        let input = Path::new(&path);
        let (enc, total_repair_symbols) = self.get_encoder(input)?;
        let original_file_hash = self.compute_original_file_hash(&input)?;
        let conn: r2d2::PooledConnection<SqliteConnectionManager> = pool.get()
            .map_err(|err| RqProcessorError::new("create_metadata", "Failed to get connection from pool", err.to_string()))?;
        if RaptorQProcessor::original_file_exists(&*conn, &original_file_hash)? {
            log::info!("Original file already exists in the database: {}! Skipping it...", original_file_hash);
            return Err(RqProcessorError::new("create_metadata", "Original file already exists in the database", original_file_hash));
        }
        let enc_config = enc.get_config().serialize().to_vec();
        log::info!("Encoder config used in `create_metadata`: {:?}", enc_config);
        // Collect symbol names
        let names: Vec<String> = enc.get_encoded_packets(total_repair_symbols)
            .iter()
            .map(|packet| RaptorQProcessor::symbols_id(&packet.serialize()))
            .collect();
        let names_len = names.len() as u32;
        // Prepare the original file metadata
        let (tx_queue, rx_queue) = crossbeam::channel::unbounded();
        RaptorQProcessor::insert_original_file_metadata(
            &original_file_hash,
            &path,
            &block_hash,
            &pastel_id,
            &enc_config,
            input.metadata().ok().map_or(0.0, |m| m.len() as f64 / 1_000_000.0),
            total_repair_symbols,
            &tx_queue,
        )?;
        // Start the worker function
        let pool_clone = pool.clone(); // Clone the pool
        std::thread::spawn(move || {
            let conn: r2d2::PooledConnection<SqliteConnectionManager> = pool_clone.get()
                .expect("Failed to get connection from pool.");
            RaptorQProcessor::insert_worker_func(rx_queue, conn)
                .expect("Insert worker failed");
        });
        // Send termination signal to the worker
        tx_queue.send(WriteOperation::Terminate).unwrap();
        Ok((
            EncoderMetaData {
                encoder_parameters: enc_config,
                source_symbols: names_len - total_repair_symbols,
                repair_symbols: total_repair_symbols,
            },
            path.clone(),
        ))
    }
    
    pub fn encode(&self, path: &String, pool: &Pool<SqliteConnectionManager>) -> Result<(EncoderMetaData, String), RqProcessorError> {
        log::info!("Starting encoding process for file: {}", path);
        let input = Path::new(&path);
        let (enc, repair_symbols) = self.get_encoder(input)?;
        log::info!("Encoder obtained with {} repair symbols.", repair_symbols);
        let original_file_hash = self.compute_original_file_hash(&input)?;
        log::info!("Original file hash computed in encoder: {}", original_file_hash);
        let (tx_queue, rx_queue) = crossbeam::channel::unbounded();
        let encoded_symbols: Vec<EncodingPacket> = enc.get_encoded_packets(repair_symbols);
        log::info!("Symbols obtained for encoding.");
        let timestamp = get_current_timestamp();
        // Iterate over symbols and send WriteOperations to worker threads
        encoded_symbols.par_iter().for_each(|symbol| {
            let pkt = symbol.serialize();
            let name = RaptorQProcessor::symbols_id(&pkt);
            let task_id = "";
            let write_op = WriteOperation::Symbol((original_file_hash.clone(), task_id.to_string(), name, pkt, timestamp));
            tx_queue.send(write_op).unwrap();
        });
        // Launch worker threads to handle the insertion of symbols
        let workers: Vec<_> = (0..NUM_WORKERS)
            .map(|_| {
                let rx_queue = rx_queue.clone();
                let pool = pool.clone();
                std::thread::spawn(move || -> Result<(), RqProcessorError> {
                    let conn = pool.get().expect("Failed to get connection from pool.");
                    RaptorQProcessor::insert_worker_func(rx_queue, conn)?;
                    Ok(())
                })
            })
            .collect();
        log::info!("Sending termination signal to insert worker(s)...");
        for _ in 0..NUM_WORKERS {
            tx_queue.send(WriteOperation::Terminate).unwrap();
        }
        // Wait for worker threads to complete
        for worker in workers {
            worker.join().expect("Worker thread panicked")?;
        }
        log::info!("Symbols inserted successfully.");
        Ok((
            EncoderMetaData {
                encoder_parameters: enc.get_config().serialize().to_vec(),
                source_symbols: encoded_symbols.len() as u32 - repair_symbols,
                repair_symbols,
            },
            path.clone(),
                ))
    }

    pub fn decode(
        &self,
        pool: &r2d2::Pool<SqliteConnectionManager>,
        encoder_parameters: &Vec<u8>,
        path_to_rq_symbol_files_for_file: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        if encoder_parameters.len() != 12 {
            return Err(Box::new(RqProcessorError::new(
                "decode",
                "encoder_parameters length must be 12",
                "".to_string(),
            )));
        }
        log::info!("Starting the decoding process...");
        let missing_hashes = Self::check_if_rq_symbol_files_are_already_in_db(
            pool,
            path_to_rq_symbol_files_for_file,
        )?;
        if !missing_hashes.is_empty() {
            log::info!("Loading missing RQ symbol files into the database...");
            Self::load_rq_symbol_files_into_db_from_folder(pool, path_to_rq_symbol_files_for_file)?;
        }
        log::info!("All RQ symbol files are now in the database. Proceeding with decoding...");
        std::thread::sleep(Duration::from_millis(250));
        let mut cfg = [0u8; 12];
        cfg.iter_mut().set_from(encoder_parameters.iter().cloned());
        let config = ObjectTransmissionInformation::deserialize(&cfg);
        let mut dec = Decoder::new(config);
        let task_id = path_to_rq_symbol_files_for_file
            .trim_end_matches('/')
            .rsplit('/')
            .nth(1)
            .ok_or_else(|| "Invalid path format")?
            .to_string();
        log::info!("Task ID: {}", task_id);
        let conn: r2d2::PooledConnection<SqliteConnectionManager> = pool.get().expect("Failed to get connection from pool.");
        let sql_statement = format!("SELECT rq_symbol_file_data FROM rq_symbols WHERE task_id = '{}'", task_id);
        let mut stmt = conn.prepare(&sql_statement)?;
        let mut symbol_data_vec = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let symbol_data: Vec<u8> = row.get(0)?;
            symbol_data_vec.push(symbol_data);
        }
        log::info!("Retrieved {} symbols from the database.", symbol_data_vec.len());
        let (tx_queue, rx_queue) = crossbeam::channel::unbounded();
        let pool_clone = pool.clone(); // Clone the pool
        std::thread::spawn(move || {
            let conn: r2d2::PooledConnection<SqliteConnectionManager> = pool_clone.get()
                .expect("Failed to get connection from pool.");
            RaptorQProcessor::insert_worker_func(rx_queue, conn)
                .expect("Insert worker failed");
        });
        let files_number = symbol_data_vec.len() as u32;
        for symbol_data in symbol_data_vec {
            if let Some(result) = dec.decode(EncodingPacket::deserialize(&symbol_data)) {
                // Construct the path for the restored file
                let restored_file_path = Path::new(path_to_rq_symbol_files_for_file)
                    .parent()
                    .ok_or_else(|| "Invalid path format")?
                    .join("restored_file");
                // Create and write to the restored file
                let mut restored_file = File::create(&restored_file_path)?;
                restored_file.write_all(&result)?;
                //Compute the hash of the restored file
                let restored_file_hash = self.compute_original_file_hash(&restored_file_path)?;
                log::info!("Restored file hash: {}", restored_file_hash);
                log::info!("Updating original file hash in the `rq_symbols` table...");
                let write_op_update_hash = WriteOperation::UpdateOriginalFileHash((task_id.clone(), restored_file_hash.clone()));
                tx_queue.send(write_op_update_hash)?;
                let conn = pool.get().expect("Failed to get connection from pool.");
                // Check if the original file data already exists in the database; if not, insert it
                if !Self::original_file_exists(&conn, &restored_file_hash)? {
                    log::info!("Original file metadata for original file hash {} doesn't exist in the database. Inserting it now...", restored_file_hash);
                    let restored_file_size_in_mb = restored_file.metadata()?.len() as f64 / 1_000_000.0; 
                    let block_hash = "NA".to_string();
                    let pastel_id = "NA".to_string();
                    let write_op_insert_original = WriteOperation::OriginalFile((restored_file_hash, restored_file_path.to_string_lossy().into_owned(), restored_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters.clone()));
                    tx_queue.send(write_op_insert_original)?;
                }
                for _ in 0..NUM_WORKERS { tx_queue.send(WriteOperation::Terminate).unwrap(); }
                let output_path_string = restored_file_path.to_string_lossy().into_owned();
                log::info!("Decoding completed successfully.");
                return Ok(output_path_string);
            }
        }
        Err(Box::new(RqProcessorError::new(
            "decode",
            "Decoding failed",
            "".to_string(),
        )))
    }
    
    pub fn decode_from_db_using_original_file_hash(
        &self,
        pool: &r2d2::Pool<SqliteConnectionManager>,
        encoder_parameters: &Vec<u8>,
        original_file_hash: &str,
    ) -> Result<String, RqProcessorError> {
        if encoder_parameters.len() != 12 {
            return Err(RqProcessorError::new(
                "decode",
                "encoder_parameters length must be 12",
                "".to_string(),
            ));
        }
        log::info!("Using original file hash in decoder: {}", original_file_hash);
        let mut cfg = [0u8; 12];
        cfg.iter_mut().set_from(encoder_parameters.iter().cloned());
        let config = ObjectTransmissionInformation::deserialize(&cfg);
        let mut dec = Decoder::new(config);
        let conn: r2d2::PooledConnection<SqliteConnectionManager> = pool.get().expect("Failed to get connection from pool.");
        let sql_statement = format!("SELECT rq_symbol_file_data FROM rq_symbols WHERE original_file_sha3_256_hash = '{}'", original_file_hash);
        let mut stmt = conn.prepare(&sql_statement)?;
        let mut symbol_data_vec = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let symbol_data: Vec<u8> = row.get(0)?;
            symbol_data_vec.push(symbol_data);
        }
        log::info!("Retrieved {} symbols from the database.", symbol_data_vec.len());

        for symbol_data in symbol_data_vec {
            if let Some(result) = dec.decode(EncodingPacket::deserialize(&symbol_data)) {
                // Construct the path for the restored file
                let restored_file_path = Path::new("/tmp/")
                    .join(original_file_hash);
                
                // Create and write to the restored file
                let mut restored_file = File::create(&restored_file_path)?;
                restored_file.write_all(&result)?;
                
                let output_path_string = restored_file_path.to_string_lossy().into_owned();
                log::info!("Decoding completed successfully.");
                return Ok(output_path_string);
            }
        }
    Err(RqProcessorError::new("decode", "Decoding failed", "".to_string()))
    }
    
    pub fn decode_with_selected_symbols(
        &self,
        conn: &Connection,
        selected_symbol_hashes: &[String],
        encoder_parameters: &Vec<u8>,
    ) -> Result<Vec<u8>, RqProcessorError> {
        if encoder_parameters.len() != 12 {
            return Err(RqProcessorError::new(
                "decode_with_selected_symbols",
                "encoder_parameters length must be 12",
                "".to_string(),
            ));
        }
        let mut cfg = [0u8; 12];
        cfg.iter_mut().set_from(encoder_parameters.iter().cloned());
        let config = ObjectTransmissionInformation::deserialize(&cfg);
        let mut dec = Decoder::new(config);
        const CHUNK_SIZE: usize = 900; // To keep below SQLite's limit
        for chunk in selected_symbol_hashes.chunks(CHUNK_SIZE) {
            // Create placeholders for the query based on the number of selected hashes in the chunk
            let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<&str>>().join(",");
            let sql_query = format!(
                "SELECT rq_symbol_file_data FROM rq_symbols WHERE rq_symbol_file_sha3_256_hash IN ({})",
                placeholders
            );
            let mut stmt = conn
                .prepare(&sql_query)
                .map_err(|err| RqProcessorError::new("decode_with_selected_symbols", "Cannot prepare statement", err.to_string()))?;
            // Create a vector of parameters to bind
            let params: Vec<&dyn ToSql> = chunk.iter().map(|x| x as &dyn ToSql).collect();
            // Execute the query with the bound parameters
            let symbol_rows = stmt
                .query_map(&*params, |row| Ok(row.get::<_, Vec<u8>>(0)?))
                .map_err(|err| RqProcessorError::new("decode_with_selected_symbols", "Cannot query symbols", err.to_string()))?;
            // Deserialize symbols into EncodingPacket objects and add them to the decoder
            for symbol_row in symbol_rows {
                let symbol_data: Vec<u8> = symbol_row.map_err(|err| RqProcessorError::new("decode_with_selected_symbols", "Cannot process symbols", err.to_string()))?;
                let symbol_packet = EncodingPacket::deserialize(&symbol_data);
                if let Some(result) = dec.decode(symbol_packet) {
                    return Ok(result);
                }
            }
        }
        Err(RqProcessorError::new(
            "decode_with_selected_symbols",
            "Decoding failed",
            "".to_string(),
        ))
    }

    fn get_encoder(&self, path: &Path) -> Result<(Encoder, u32), RqProcessorError> {
        let mut file = File::open(&path).map_err(|err| RqProcessorError::new_file_err("get_encoder", "Cannot open file", path, err.to_string()))?;
        let source_size = file.metadata().map_err(|err| RqProcessorError::new_file_err("get_encoder", "Cannot access metadata of file", path, err.to_string()))?.len();
        let mut data = Vec::with_capacity(source_size as usize);
        file.read_to_end(&mut data).map_err(|err| RqProcessorError::new_file_err("get_encoder", "Cannot read input file", path, err.to_string()))?;
        let config = ObjectTransmissionInformation::with_defaults(
            source_size,
            self.symbol_size,
        );
        let encoder = Encoder::new(&data, config);
        let repair_symbols_num = RaptorQProcessor::repair_symbols_num(self.symbol_size, self.redundancy_factor, source_size);
        Ok((encoder, repair_symbols_num))
    }

    fn repair_symbols_num(symbol_size: u16, redundancy_factor: u8, data_len: u64) -> u32 {
        if data_len <= symbol_size as u64 {
            return redundancy_factor as u32;
        }
        let source_symbols = (data_len as f64 / symbol_size as f64).ceil();
        let repair_symbols = (source_symbols * (redundancy_factor as f64 - 1.0)).ceil() as u32;
        repair_symbols
    }
    
    fn symbols_id(symbol: &Vec<u8>) -> String {
        let mut hasher = Sha3_256::new();
        hasher.update(symbol);
        let hash_result = hasher.finalize();
        bs58::encode(&hash_result).into_string()
    }

}


// Struct and Type Definitions
#[derive(Debug, Clone)]
pub struct EncoderMetaData {
    pub encoder_parameters: Vec<u8>,
    pub source_symbols: u32,
    pub repair_symbols: u32
}

#[derive(Serialize, Deserialize)]
struct RqIdsFile {
    id: String,
    block_hash: String,
    pastel_id: String,
    symbol_identifiers: Vec<String>
}


// Error Implementations

impl From<io::Error> for RqProcessorError {
    fn from(error: io::Error) -> Self {
        RqProcessorError {
            func: "RQProcessorError".to_string(),
            msg: String::new(),
            prev_msg: error.to_string()
        }
    }
}

impl From<String> for RqProcessorError {
    fn from(error: String) -> Self {
        RqProcessorError {
            func: "RQProcessorError".to_string(),
            msg: error,
            prev_msg: String::new()
        }
    }
}

impl From<&str> for RqProcessorError {
    fn from(error: &str) -> Self {
        RqProcessorError {
            func: "RQProcessorError".to_string(),
            msg: error.to_string(),
            prev_msg: String::new()
        }
    }
}

impl From<serde_json::Error> for RqProcessorError {
    fn from(error: serde_json::Error) -> Self {
        RqProcessorError {
            func: "RQProcessorError".to_string(),
            msg: String::new(),
            prev_msg: error.to_string()
        }
    }
}

impl From<rusqlite::Error> for RqProcessorError {
    fn from(error: rusqlite::Error) -> Self {
        RqProcessorError {
            func: "RQProcessorError".to_string(),
            msg: String::new(),
            prev_msg: error.to_string(),
        }
    }
}

impl From<r2d2::Error> for RqProcessorError {
    fn from(error: r2d2::Error) -> Self {
        RqProcessorError {
            func: "RQProcessorError".to_string(),
            msg: String::new(),
            prev_msg: error.to_string(),
        }
    }
}

impl From<Box<dyn std::error::Error>> for RqProcessorError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        RqProcessorError::new("Conversion", "Error conversion occurred", error.to_string())
    }
}


// Test Code
#[cfg(test)]
pub mod tests {
    use env_logger;
    use serial_test::serial;

    fn setup() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
    }
    
    const TEST_DB_PATH: &str = "/home/ubuntu/rqservice/test_files/test_rq_symbols.sqlite";

    impl From<RqProcessorError> for TestError {
        fn from(err: RqProcessorError) -> Self {
            TestError::RqProcessorError(err)
        }
    }
    
    impl From<rusqlite::Error> for TestError {
        fn from(err: rusqlite::Error) -> TestError {
            TestError::DbError(err)
        }
    }

    impl From<std::io::Error> for TestError {
        fn from(err: std::io::Error) -> Self {
            TestError::Other(Box::new(err))
        }
    }
    
    #[derive(Debug)]
    enum TestError {
        MetaError(String),
        EncodeError(String),
        DecodeError(String),
        RqProcessorError(RqProcessorError),
        DbError(rusqlite::Error),
        Other(Box<dyn std::error::Error>), // Generic error type to cover other errors
    }

    use super::*;
    use std::time::Instant;
    use std::fs::File;
    use std::io::Write;
    use rand::Rng;
    use tempfile::tempdir;
    use std::sync::Mutex;

    lazy_static::lazy_static! {
        static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
    }
    
    // Utility Functions
    fn setup_pool() -> Pool<SqliteConnectionManager> {
        log::info!("Attempting to open Sqlite Pool for TEST_DB_PATH at path: {}", TEST_DB_PATH); 
        let manager = SqliteConnectionManager::file(TEST_DB_PATH); // Use the test database path
        Pool::new(manager).expect("Failed to create pool.")
    }

    fn test_meta(pool: &Pool<SqliteConnectionManager>, path: String, size: u32) -> Result<(EncoderMetaData, String), TestError> {
        log::info!("Testing file {}", path);
        let processor = RaptorQProcessor::new(TEST_DB_PATH).unwrap();    
        let encode_time = Instant::now();
        match processor.create_metadata(&path, &String::from("12345"), &String::from("67890"), pool) {
                Ok((meta, path)) => {
                log::info!("source symbols = {}; repair symbols = {}", meta.source_symbols, meta.repair_symbols);
                let source_symbols = (size as f64 / 50_000.0f64).ceil() as u32;
                assert_eq!(meta.source_symbols, source_symbols);
                log::info!("{:?} spent to create symbols", encode_time.elapsed());
                Ok((meta, path))
            }
            Err(e) => Err(TestError::MetaError(format!(
                "create_metadata returned Error - {:?}",
                e
            ))),
        }
    }
        
    fn test_encode(pool: &Pool<SqliteConnectionManager>, path: String, size: u32) -> Result<(EncoderMetaData, String), TestError> {
        log::info!("Testing file {}", path);
        let processor = RaptorQProcessor::new(TEST_DB_PATH).unwrap();    
        let encode_time = Instant::now(); // Define the encode_time variable here
        match processor.encode(&path, pool) {
            Ok((meta, db_path)) => {
                log::info!("source symbols = {}; repair symbols = {}", meta.source_symbols, meta.repair_symbols);
                let source_symbols = (size as f64 / 50_000.0f64).ceil() as u32;
                assert_eq!(meta.source_symbols, source_symbols);
                log::info!("{:?} spent to create symbols", encode_time.elapsed()); // Now encode_time is in scope
                Ok((meta, db_path))
            },
            Err(e) => Err(TestError::EncodeError(format!("encode returned Error - {:?}", e))),
        }
    }
        
    fn test_decode(pool: &Pool<SqliteConnectionManager>, encoder_parameters: &Vec<u8>, original_file_hash: &str) -> Result<(), TestError> {
        log::info!("Testing file with original hash {}", original_file_hash);
        let processor = RaptorQProcessor::new(TEST_DB_PATH).unwrap();    
        match processor.decode_from_db_using_original_file_hash(&pool, encoder_parameters, original_file_hash) {
            Ok(output_path) => {
                log::info!("Restored file path: {}", output_path);
                Ok(())
            },
            Err(e) => Err(TestError::DecodeError(format!("decode returned Error - {:?}", e))),
        }
    }

    impl From<Box<dyn std::error::Error>> for TestError {
        fn from(err: Box<dyn std::error::Error>) -> Self {
            TestError::Other(err)
        }
    }

    fn create_random_file(file_path: &str, size: usize) -> std::io::Result<()> {
        let mut file = File::create(file_path)?;
        let mut rng = rand::thread_rng();
        let data: Vec<u8> = (0..size).map(|_| rng.gen()).collect();
        file.write_all(&data)?;
        Ok(())
    }

    #[test]
    fn rq_test_metadata() -> Result<(), TestError> {
        setup();
        let dir = tempdir()?;
        let file_path = dir.path().join("10_000_000");
        create_random_file(file_path.to_str().unwrap(), 10_000_000)?;

        let pool = setup_pool();
        test_meta(&pool, file_path.to_str().unwrap().to_string(), 10_000_000)?;
        Ok(())
    }

    #[test]
    fn rq_test_encode_decode() -> Result<(), TestError> {
        setup();
        initialize_database(TEST_DB_PATH).unwrap();
        let dir = tempdir()?;
        let file_path = dir.path().join("10_000_000");
        log::info!("Creating random file at path: {}", file_path.to_str().unwrap());
        create_random_file(file_path.to_str().unwrap(), 10_000_000)?;
        log::info!("Random file created successfully.");
        log::info!("Starting encoding process...");
        let pool = setup_pool();
        let (meta, _path) = test_encode(&pool, file_path.to_str().unwrap().to_string(), 10_000_000)?;
        log::info!("Encoding process completed successfully.");

        let processor = RaptorQProcessor::new(TEST_DB_PATH).unwrap();    
        let original_file_hash = processor.compute_original_file_hash(&file_path)?;
        log::info!("Original file hash computed in test: {}", original_file_hash);
        log::info!("Starting decoding process...");
        test_decode(&pool, &meta.encoder_parameters, &original_file_hash)?;

        Ok(())
    }


    #[test]
    fn test_compute_original_file_hash() {
        setup();
        use std::fs::File;
        use std::io::Write;
        use rand::Rng;
        use tempfile::tempdir;
        use RaptorQProcessor;
    
        initialize_database(TEST_DB_PATH).unwrap();
        // Sizes to test: one that's a multiple of the chunk size and one that's not
        let sizes_to_test = [256 * 1024, 256 * 1024 + 1];
    
        let processor = RaptorQProcessor::new(TEST_DB_PATH).unwrap();    
        for size in sizes_to_test.iter() {
            // Create a temporary directory
            let dir = tempdir().expect("Failed to create temp dir");
            let file_path = dir.path().join("test_file");
    
            // Create and write random data to the file
            let mut file = File::create(&file_path).expect("Failed to create file");
            let mut rng = rand::thread_rng();
            let data: Vec<u8> = (0..*size).map(|_| rng.gen()).collect();
            file.write_all(&data).expect("Failed to write data");
    
            // Test the compute_original_file_hash method
            let result = processor.compute_original_file_hash(&file_path);
            assert!(result.is_ok(), "Failed for size {}", size);
    
            // Temp directory will be deleted when dir goes out of scope
        }
    }

    // End to end test
    pub mod end_to_end_tests  {
        use super::*;
        use rand::Rng;
        use std::fs;
        use rand::prelude::SliceRandom;
        use std::time::Instant;


        const TEST_DB_PATH: &str = "/home/ubuntu/rqservice/test_files/test_rq_symbols.sqlite";
        const STATIC_TEST_FILE: &str = "/home/ubuntu/rqservice/test_files/input_test_file.jpg"; // Path to a real sample file
        // const STATIC_TEST_FILE: &str = "/home/ubuntu/rqservice/test_files/cp_detector.7z"; // Path to a real sample file


        fn generate_test_file() -> Result<(String, Vec<u8>), Box<dyn std::error::Error>> {
            let file_name = "input_test_file.txt";
            let mut rng = rand::thread_rng();
            let input_test_file_data: Vec<u8> = (0..1024).map(|_| rng.gen()).collect();
            fs::write(file_name, &input_test_file_data)?;
            Ok((file_name.to_string(), input_test_file_data))
        }

        fn sha3_256_hash(data: &[u8]) -> String {
            let mut hasher = Sha3_256::new();
            hasher.update(data);
            let hash_result = hasher.finalize();
            bs58::encode(&hash_result).into_string()
        }

        #[derive(Deserialize)]
        struct Config {
            #[allow(dead_code)]
            #[serde(rename = "grpc-service")]
            grpc_service: String,
            #[serde(rename = "symbol-size")]
            symbol_size: u16,
            #[allow(dead_code)]
            #[serde(rename = "redundancy-factor")]
            redundancy_factor: u8,
        }

        #[test]
        #[serial]
        pub fn test_rqprocessor() -> Result<(), Box<dyn std::error::Error>> {
                    
            let start_time = Instant::now(); // Mark the start time
            initialize_database(TEST_DB_PATH).unwrap();

            setup();
            // Read the configuration
            log::info!("Reading rqconfig.toml at path: rqconfig.toml");
            let config = toml::from_str::<Config>(&fs::read_to_string(&**RQ_CONFIG_PATH)?)?;

            // Choose between using a static test file or generating a random test file
            log::info!("Opening STATIC_TEST_FILE at path: {}", STATIC_TEST_FILE);
            let (input_test_file, input_test_file_data) = if Path::new(STATIC_TEST_FILE).exists() {
                (STATIC_TEST_FILE.to_string(), fs::read(STATIC_TEST_FILE)?)
            } else {
                generate_test_file()?
            };

            log::info!("Opening TEST_DB_PATH at path: {}", TEST_DB_PATH); 
            // Initialize database
            let conn = RaptorQProcessor::initialize_db(TEST_DB_PATH)?;

            log::info!("Creating RaptorQProcessor now..."); 
            // Create processor
            let processor: RaptorQProcessor = RaptorQProcessor::new(TEST_DB_PATH).unwrap();                        
            log::info!("RaptorQProcessor created.");

            // Compute original file hash
            let original_file_hash = sha3_256_hash(&input_test_file_data);
            log::info!("Original file hash computed: {}", original_file_hash);

            // Create metadata
            log::info!("Creating metadata and storing...");
            let pool = setup_pool();
            log::info!("Pool created.");
            let (metadata, _path) = processor.create_metadata(
                &input_test_file,
                &"block_hash".to_string(),
                &"pastel_id".to_string(),
                &pool,
            )?;
            log::info!("Metadata created and stored.");

            // Encode
            log::info!("Encoding...");
            let (encoder_metadata_2, _path) = processor.encode(&input_test_file, &pool)?;
            assert_eq!(metadata.encoder_parameters, encoder_metadata_2.encoder_parameters, "Encoder parameters do not match");
            log::info!("Encoding completed successfully.");

            // Attempt to create metadata and store again for the same file
            log::info!("Attempting to create metadata and store again...");
            let result = processor.create_metadata(
                &input_test_file,
                &"block_hash".to_string(),
                &"pastel_id".to_string(),
                &pool,
            );

            // Verify that the correct error message is returned
            match result {
                Err(RqProcessorError { func, msg, prev_msg: _ }) if func == "create_metadata" && msg.starts_with("Original file already exists") => {
                    log::info!("Received expected error message: {}", msg);
                }
                _ => {
                    panic!("Unexpected result when attempting to create metadata and store again: {:?}", result);
                }
            }

            //__________________________________________________________________________________ Decoding phase:
            log::info!("First attempting to decode using ALL generated RQ symbol files...");
            // Debugging step: Decode using all symbols
            log::info!("Now attempting to decode original file using all symbols and verifying...");
            // Sleep for 1 second to allow the database to finish writing
            std::thread::sleep(Duration::from_secs(1)); 
            let decoded_file_path = processor.decode_from_db_using_original_file_hash(&pool, &encoder_metadata_2.encoder_parameters, &original_file_hash)?;
            let decoded_file_data_all_symbols = fs::read(&decoded_file_path)?;
            let decoded_file_hash_all_symbols = sha3_256_hash(&decoded_file_data_all_symbols);
            log::info!("Decoded file hash (all symbols): {}", decoded_file_hash_all_symbols);
            assert_eq!(original_file_hash, decoded_file_hash_all_symbols, "Reconstructed file hash does not match original file hash using all symbols");
            log::info!("Reconstructed file hash matches original file hash using the decode method!");
            
            log::info!("Now attempting to decode original file using a random subset of RQ symbol files...");
            // Number of random decoding attempts
            let attempts = 10;
            // Original file size in bytes
            let original_file_size = input_test_file_data.len();
            // Size of each symbol file in bytes
            let symbol_size = config.symbol_size;
            // Calculate the required size with a small cushion (e.g., 1.005 for 100.5%)
            let required_size = (original_file_size as f64 * 1.005).ceil() as usize;
            // Calculate the number of symbols to fetch based on the required size and symbol size
            let symbols_to_fetch = (required_size as f64 / symbol_size as f64).ceil() as usize;
            log::info!("Number of random decoding attempts to try: {}", attempts);
            log::info!("Number of symbols to fetch for decoding: {} (compared to total number of generated RQ symbol files of {})", symbols_to_fetch, metadata.source_symbols + metadata.repair_symbols);
            log::info!("Now attempting to reconstruct original file {} {} times...", STATIC_TEST_FILE, attempts);
            for attempt_number in 0..attempts {
                log::info!("Attempt {}...", attempt_number);
                // Prepare the statement to select symbol hashes
                let mut stmt = conn.prepare("SELECT rq_symbol_file_sha3_256_hash FROM rq_symbols WHERE original_file_sha3_256_hash = ?1")?;
                log::info!("Statement prepared: {:?}", stmt);
                // Query the symbol hashes with the original file hash
                let all_symbol_hashes: Result<Vec<String>, _> = stmt.query_map(params![&original_file_hash], |row| {
                    row.get(0)
                })?.collect();
                // Unwrap the result or handle the error as needed
                let all_symbol_hashes = all_symbol_hashes?;
                // Randomly select a subset of symbol hashes
                let mut rng = rand::thread_rng();
                log::info!("Randomly selecting a subset of {} symbol hashes...", symbols_to_fetch);
                let selected_symbol_hashes: Vec<String> = all_symbol_hashes.choose_multiple(&mut rng, symbols_to_fetch).cloned().collect();
                assert_eq!(selected_symbol_hashes.len(), symbols_to_fetch, "Selected symbol hashes count does not match symbols_to_fetch");
                // Decode using the selected subset of symbol hashes and verify
                log::info!("Now attempting to decode using the selected subset and verifying...");
                let decoded_file_data = processor.decode_with_selected_symbols(&conn, &selected_symbol_hashes, &metadata.encoder_parameters)?;
                let decoded_file_hash = sha3_256_hash(&decoded_file_data);
                log::info!("Decoded file hash: {}", decoded_file_hash);
                log::info!("Original file hash: {}", original_file_hash);
                log::info!("Decoded file size: {}", decoded_file_data.len());
                assert_eq!(original_file_hash, decoded_file_hash, "Reconstructed file hash does not match original file hash");
                log::info!("Reconstructed file hash matches original file hash! Trying again...");
            }
            log::info!("All attempts successful!");
            log::info!("Cleaning up...");
            // Clean up (optional)
            if !Path::new(STATIC_TEST_FILE).exists() {
                fs::remove_file(input_test_file)?;
            }
            fs::remove_file(TEST_DB_PATH)?;
            log::info!("Clean up complete!");
            let elapsed_time = start_time.elapsed();  // Calculate the elapsed time
            log::info!("End-to-End Test total execution time: {:?}", elapsed_time);
            Ok(())
        }            
}
}

// Copyright (c) 2021-2023 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};
use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use std::time::Duration;
use std::{fmt, fs, io};
use std::sync::Mutex;
use chrono::{Duration as ChronoDuration, Utc};
use serde_derive::{Deserialize, Serialize};
use rusqlite::{Connection, params, ToSql};
use rayon::prelude::*;
use rand::Rng;
use lazy_static::lazy_static;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use toml;

lazy_static! {
    static ref WRITE_LOCK: Mutex<()> = Mutex::new(());
}

const MAX_DAYS_OLD_BEFORE_RQ_SYMBOL_FILES_ARE_PURGED: i64 = 2; // Adjust as needed
pub const NUM_WORKERS: usize = 1;
pub const ORIGINAL_FILE_LOAD_CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10 MB
pub const DB_PATH: &str = crate::DB_PATH;
pub const RQ_CONFIG_PATH: &str = "/home/ubuntu/rqservice/examples/rqconfig.toml";


#[derive(Deserialize)]
struct RqConfig {
    #[serde(rename = "grpc-service")]
    grpc_service: String,
    #[serde(rename = "symbol-size")]
    symbol_size: u16,
    #[serde(rename = "redundancy-factor")]
    redundancy_factor: u8,
}

fn read_config() -> Result<RqConfig, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(RQ_CONFIG_PATH)?;
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
    OriginalFile((String, String, f64, i32, String, String, Vec<u8>)),
    Symbol((String, String, Vec<u8>, i64)),
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
        const CHUNK_SIZE: usize = 256 * 1024; // 256 KB
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
        set_pragma_if_different!("synchronous", "NORMAL");
        set_pragma_if_different!("cache_size", "-524288");
        set_pragma_if_different!("busy_timeout", "2000");
        set_pragma_if_different!("wal_autocheckpoint", "100");
        log::debug!("Creating tables");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rq_symbols (
                original_file_sha3_256_hash TEXT,
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
        symbols: &[(String, String, Vec<u8>, i64)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO rq_symbols (original_file_sha3_256_hash, rq_symbol_file_sha3_256_hash, rq_symbol_file_data, utc_datetime_symbol_file_created) VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (original_file_hash, symbol_hash, symbol_data, timestamp) in symbols.iter() {
                let rows_affected = stmt.execute(params![original_file_hash, symbol_hash, symbol_data, timestamp])?;
                if rows_affected == 0 {
                    log::info!("Symbol with hash {} already exists in the database. Skipping insertion.", symbol_hash);
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
    
    fn insert_original_file(tx: &rusqlite::Transaction, original_file_hash: &str, original_file_path: &str, original_file_size_in_mb: f64, files_number: i32, block_hash: &str, pastel_id: &str, encoder_parameters: &[u8]) -> Result<(), rusqlite::Error> {
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
        symbol_batch: &[(String, String, Vec<u8>, i64)],
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
    
    fn insert_worker_func(
        rx_queue: crossbeam::channel::Receiver<WriteOperation>,
        mut conn: r2d2::PooledConnection<SqliteConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        const MAX_RETRIES: u32 = 5;
        const BATCH_SIZE: usize = 25000;
        const RETRY_DELAY: Duration = Duration::from_millis(50);
        let mut symbol_batch: Vec<(String, String, Vec<u8>, i64)> = Vec::with_capacity(BATCH_SIZE);
        log::info!("Insert worker started with settings: MAX_RETRIES = {}, BATCH_SIZE = {}, RETRY_DELAY = {:?}", MAX_RETRIES, BATCH_SIZE, RETRY_DELAY);
        for write_op in rx_queue.iter() {
            match write_op {
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
                            }
                            Err(_) if retries < MAX_RETRIES => {
                                retries += 1;
                                let jitter = rand::thread_rng().gen_range(0..10);
                                std::thread::sleep(RETRY_DELAY + Duration::from_millis(jitter));
                            }
                            Err(err) => return Err(err.into()), // Convert the error into a boxed error
                            }
                    }
                },
                WriteOperation::Symbol(symbol_data) => {
                    symbol_batch.push(symbol_data);
                    if symbol_batch.len() == BATCH_SIZE {
                        RaptorQProcessor::insert_symbols_with_retry(&mut conn, &symbol_batch)?;
                        symbol_batch.clear();
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
        RaptorQProcessor::insert_symbols_with_retry(&mut conn, &symbol_batch)?;
        // Get the underlying SQLite connection
        let sqlite_conn: &rusqlite::Connection = &*conn;
        // Execute a manual WAL checkpoint
        let _: (i32, i32, i32) = sqlite_conn.query_row("PRAGMA wal_checkpoint(FULL);", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    }
    log::info!("Insert worker function completed successfully.");
    Ok(())
    }

    fn retrieve_original_file(conn: &Connection, original_file_hash: &str) -> Result<(String, f64, i32, Vec<u8>), rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT original_file_path, original_file_size_in_mb, files_number, encoder_parameters FROM original_files WHERE original_file_sha3_256_hash = ?1")?;
        let mut rows = stmt.query(params![original_file_hash])?;
        if let Some(row) = rows.next()? {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        } else {
            Err(rusqlite::Error::QueryReturnedNoRows)
        }
    }

    fn original_file_exists(conn: &Connection, original_file_hash: &str) -> Result<bool, rusqlite::Error> {
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM original_files WHERE original_file_sha3_256_hash = ?1",
            params![original_file_hash],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn create_metadata_and_store(
        &self,
        path: &String,
        block_hash: &String,
        pastel_id: &String,
        pool: &Pool<SqliteConnectionManager>,
    ) -> Result<(EncoderMetaData, String), RqProcessorError> {
        let input = Path::new(&path);
        let original_file_hash = self.compute_original_file_hash(&input)?;
        let conn: r2d2::PooledConnection<SqliteConnectionManager> = pool.get()
            .map_err(|err| RqProcessorError::new("create_metadata_and_store", "Failed to get connection from pool", err.to_string()))?;
        if RaptorQProcessor::original_file_exists(&*conn, &original_file_hash)? {
            log::info!("Original file already exists in the database: {}! Skipping it...", original_file_hash);
            return Err(RqProcessorError::new("create_metadata_and_store", "Original file already exists in the database", original_file_hash));
        }
        // Open the file to get metadata
        let file = File::open(&input)?;
        let source_size: u64 = file.metadata()?.len();
        let enc = self.get_encoder(input)?; // Get the encoder using the new get_encoder method
        let enc_config = enc.get_config().serialize().to_vec(); // Serialize the configuration
        let total_repair_symbols = RaptorQProcessor::repair_symbols_num(self.symbol_size, self.redundancy_factor, source_size);
        let chunk_repair_symbols = total_repair_symbols / self.redundancy_factor as u32;
        let mut cumulative_repair_symbols = 0;
        let total_symbols = total_repair_symbols;
        let mut remaining_symbols = total_symbols;
        log::info!("Total symbols to insert: {}", total_symbols);
        log::info!("Remaining symbols to process: {} out of {}.", remaining_symbols, total_symbols);
        let original_file_metadata = (
            original_file_hash.clone(),
            path.clone(),
            input.metadata().ok().map_or(0.0, |m| m.len() as f64 / 1_000_000.0),
            total_repair_symbols as i32,
            block_hash.clone(),
            pastel_id.clone(),
            vec![], // Will be populated later
        );
        let (tx_queue, rx_queue) = crossbeam::channel::unbounded();
        tx_queue.send(WriteOperation::OriginalFile(original_file_metadata)).unwrap();
        for chunk in 0..self.redundancy_factor {
            log::info!("Encoding and inserting chunk {}/{}.", chunk + 1, self.redundancy_factor);
            // Create a new encoder for this chunk by reading the file
            let enc = self.get_encoder(input)?;
            // Calculate the number of new symbols to generate for this chunk
            let new_repair_symbols = chunk_repair_symbols * (chunk as u32 + 1) - cumulative_repair_symbols;
            // Get the encoded packets for this chunk, based on the new symbols required
            let encoded_packets = enc.get_encoded_packets(new_repair_symbols + cumulative_repair_symbols);
            // Take the new repair symbols that have not been processed yet
            let new_encoded_packets = encoded_packets
                .iter()
                .skip(cumulative_repair_symbols as usize)
                .take(new_repair_symbols as usize)
                .collect::<Vec<_>>();
            let new_encoded_packets_len = new_encoded_packets.len() as u32;
            cumulative_repair_symbols += new_encoded_packets_len; // Update the cumulative count
            remaining_symbols = total_symbols - cumulative_repair_symbols; // Update remaining symbols
            log::info!("Remaining symbols to process: {} out of {}.", remaining_symbols, total_symbols);
            // Prepare the symbols and files (use new_encoded_packets instead of encoded_packets)
            let symbols_and_files: Vec<_> = new_encoded_packets
                .par_iter()
                .map(|packet| (
                    original_file_hash.clone(),
                    RaptorQProcessor::symbols_id(&packet.serialize()),
                    packet.serialize(),
                    get_current_timestamp(),
                ))
                .collect();
            // Start worker threads for this chunk
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
            log::info!("Sending symbols to insert worker(s)...");
            for symbol_data in symbols_and_files {
                tx_queue.send(WriteOperation::Symbol(symbol_data)).unwrap();
            }
            log::info!("Sending termination signal to insert worker(s)...");
            for _ in 0..NUM_WORKERS {
                tx_queue.send(WriteOperation::Terminate).unwrap();
            }
            log::info!("Waiting for workers to complete...");
            for worker in workers {
                worker.join().expect("Worker thread panicked");
            }
        }
        log::info!("Original file metadata and symbols inserted successfully.");
        Ok((
            EncoderMetaData {
                encoder_parameters: enc_config, // Use the serialized configuration extracted earlier
                source_symbols: (source_size as u32) / (self.symbol_size as u32), // Calculating source symbols based on file size
                repair_symbols: total_repair_symbols - (source_size as u32) / (self.symbol_size as u32) // We adjust the repair symbols count here to reflect the actual number of repair symbols generated beyond the source symbols
            },
            original_file_hash,
        ))
    }
    
    pub fn encode(&self, path: &String, pool: &Pool<SqliteConnectionManager>) -> Result<(EncoderMetaData, String), RqProcessorError> {
        log::info!("Starting encoding process for file: {}", path);
        let input = Path::new(&path);
        let enc = self.get_encoder(input)?;
        let repair_symbols = RaptorQProcessor::repair_symbols_num(self.symbol_size, self.redundancy_factor, input.metadata().unwrap().len());
        log::info!("Encoder obtained with {} repair symbols.", repair_symbols);
        let original_file_hash = self.compute_original_file_hash(&input)?;
        log::info!("Original file hash computed in encoder: {}", original_file_hash);
        let (tx_queue, rx_queue) = crossbeam::channel::unbounded();
        let encoded_symbols: Vec<EncodingPacket> = enc.get_encoded_packets(repair_symbols);
        log::info!("Symbols obtained for encoding.");
        let timestamp = get_current_timestamp();
        // Prepare symbol data for worker threads
        let symbol_data: Vec<_> = encoded_symbols.par_iter()
            .map(|symbol| {
                let pkt = symbol.serialize();
                let name = RaptorQProcessor::symbols_id(&pkt);
                WriteOperation::Symbol((original_file_hash.clone(), name, pkt, timestamp))
            })
            .collect();
        // Send symbol data to worker threads
        for data in symbol_data {
            tx_queue.send(data).unwrap();
        }
        // Launch worker threads to handle the insertion of symbols
        let workers: Vec<_> = (0..NUM_WORKERS)
            .map(|_| {
                let rx_queue = rx_queue.clone();
                let pool = pool.clone();
                std::thread::spawn(move || -> Result<(), RqProcessorError> {
                    let conn = pool.get().expect("Failed to get connection from pool.");
                    RaptorQProcessor::insert_worker_func(rx_queue, conn)?; // Call the new function here
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
        // Get a connection from the pool
        let conn = pool.get().expect("Failed to get connection from pool.");
        // Execute a manual WAL checkpoint
        let _: (i32, i32, i32) = conn.query_row("PRAGMA wal_checkpoint(FULL);", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
        Ok(
            (EncoderMetaData {
                encoder_parameters: enc.get_config().serialize().to_vec(),
                source_symbols: encoded_symbols.len() as u32 - repair_symbols,
                repair_symbols},
            DB_PATH.to_string() // Update this to the appropriate path or details
            )
        )
    }
    
    pub fn decode(&self, conn: &Connection, encoder_parameters: &Vec<u8>, original_file_hash: &str) -> Result<String, RqProcessorError> {
        if encoder_parameters.len() != 12 {
            return Err(RqProcessorError::new("decode", "encoder_parameters length must be 12", "".to_string()));
        }
        log::info!("Using original file hash in decoder: {}", original_file_hash);
        let (original_file_path, _, _, _) = Self::retrieve_original_file(conn, original_file_hash)?;
        let mut cfg = [0u8; 12];
        cfg.copy_from_slice(encoder_parameters);
        let config = ObjectTransmissionInformation::deserialize(&cfg);
        let mut dec = Decoder::new(config);
        let mut stmt = conn.prepare("SELECT rq_symbol_file_data FROM rq_symbols WHERE original_file_sha3_256_hash = ?1")
            .map_err(|err| RqProcessorError::new("decode", "Cannot prepare statement", err.to_string()))?;
        let symbol_rows = stmt.query_map(params![original_file_hash], |row| Ok(row.get(0)?))
            .map_err(|err| RqProcessorError::new("decode", "Cannot query symbols", err.to_string()))?;
        // Deserialize symbols into EncodingPacket objects and add them to the decoder
        for symbol_row in symbol_rows {
            let symbol_data: Vec<u8> = symbol_row.map_err(|err| RqProcessorError::new("decode", "Cannot process symbols", err.to_string()))?;
            let symbol_packet = EncodingPacket::deserialize(&symbol_data);
            dec.add_new_packet(symbol_packet);
        }
        // Retrieve the result
        let result = dec.get_result().ok_or_else(|| RqProcessorError::new("decode", "Decoding failed", "".to_string()))?;
        // Write decoded content to a file
        let original_file_name = Path::new(&original_file_path)
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("");
        let reconstructed_dir = Path::new("reconstructed_files").canonicalize().unwrap_or_else(|_| std::path::PathBuf::from("reconstructed_files"));
        let output_path_buf = reconstructed_dir.join(original_file_name);
        // Create the directory if it doesn't exist
        std::fs::create_dir_all(&reconstructed_dir)?;
        let mut file = File::create(&output_path_buf)
            .map_err(|err| RqProcessorError::new_file_err("decode", "Cannot create output file", &output_path_buf, err.to_string()))?;
        file.write_all(&result)?;
        // Convert PathBuf to String
        let output_path_string = output_path_buf.to_string_lossy().into_owned();
        Ok(output_path_string)
    }

    pub fn decode_with_selected_symbols(&self, conn: &Connection, selected_symbol_hashes: &[String], encoder_parameters: &Vec<u8>) -> Result<Vec<u8>, RqProcessorError> {
        if encoder_parameters.len() != 12 {
            return Err(RqProcessorError::new("decode_with_selected_symbols", "encoder_parameters length must be 12", "".to_string()));
        }
        let mut cfg = [0u8; 12];
        cfg.copy_from_slice(encoder_parameters);
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
            let mut stmt = conn.prepare(&sql_query)
                .map_err(|err| RqProcessorError::new("decode_with_selected_symbols", "Cannot prepare statement", err.to_string()))?;
            // Create a vector of parameters to bind
            let params: Vec<&dyn ToSql> = chunk.iter().map(|x| x as &dyn ToSql).collect();
            // Execute the query with the bound parameters
            let symbol_rows = stmt.query_map(&*params, |row| Ok(row.get::<_, Vec<u8>>(0)?))
                .map_err(|err| RqProcessorError::new("decode_with_selected_symbols", "Cannot query symbols", err.to_string()))?;
            // Deserialize symbols into EncodingPacket objects and add them to the decoder
            for symbol_row in symbol_rows {
                let symbol_data: Vec<u8> = symbol_row.map_err(|err| RqProcessorError::new("decode_with_selected_symbols", "Cannot process symbols", err.to_string()))?;
                let symbol_packet = EncodingPacket::deserialize(&symbol_data);
                dec.add_new_packet(symbol_packet);
            }
        }
        // Retrieve the result
        let result = dec.get_result().ok_or_else(|| RqProcessorError::new("decode_with_selected_symbols", "Decoding failed", "".to_string()))?;
        Ok(result)
    }

    fn get_encoder(&self, path: &Path) -> Result<Encoder, RqProcessorError> {
        let mut file = File::open(&path).map_err(|err| RqProcessorError::new_file_err("get_encoder", "Cannot open file", path, err.to_string()))?;
        let source_size = file.metadata().map_err(|err| RqProcessorError::new_file_err("get_encoder", "Cannot access metadata of file", path, err.to_string()))?.len();
        let mut data = Vec::with_capacity(source_size as usize);
        file.read_to_end(&mut data).map_err(|err| RqProcessorError::new_file_err("get_encoder", "Cannot read input file", path, err.to_string()))?;
        let mut builder: raptorq::EncoderBuilder = raptorq::EncoderBuilder::new();
        builder.set_max_packet_size(self.symbol_size);
        // You can set other parameters if needed
        let encoder = builder.build(&data);
        Ok(encoder)
    }
    
    fn repair_symbols_num(symbol_size: u16, redundancy_factor: u8, data_len: u64) -> u32 {
        let source_symbols = (data_len as f64 / symbol_size as f64).ceil();
        let repair_symbols = (source_symbols * redundancy_factor as f64).ceil() as u32;
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
mod tests {
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
        match processor.create_metadata_and_store(&path, &String::from("12345"), &String::from("67890"), pool) {
                Ok((meta, path)) => {
                log::info!("source symbols = {}; repair symbols = {}", meta.source_symbols, meta.repair_symbols);
                let source_symbols = (size as f64 / 50_000.0f64).ceil() as u32;
                assert_eq!(meta.source_symbols, source_symbols);
                log::info!("{:?} spent to create symbols", encode_time.elapsed());
                Ok((meta, path))
            }
            Err(e) => Err(TestError::MetaError(format!(
                "create_metadata_and_store returned Error - {:?}",
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
        let conn = pool.get().expect("Failed to get connection from pool.");
        log::info!("Testing file with original hash {}", original_file_hash);
        let processor = RaptorQProcessor::new(TEST_DB_PATH).unwrap();    
        match processor.decode(&conn, encoder_parameters, original_file_hash) {
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
        create_random_file(file_path.to_str().unwrap(), 10_000_000)?;

        let pool = setup_pool();
        let (meta, _path) = test_encode(&pool, file_path.to_str().unwrap().to_string(), 10_000_000)?;

        let processor = RaptorQProcessor::new(TEST_DB_PATH).unwrap();    
        let original_file_hash = processor.compute_original_file_hash(&file_path)?;

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
    mod end_to_end_tests  {
        use super::*;
        use rand::Rng;
        use std::fs;
        use rand::prelude::SliceRandom;
        use std::time::Instant;


        const TEST_DB_PATH: &str = "/home/ubuntu/rqservice/test_files/test_rq_symbols.sqlite";
        // const STATIC_TEST_FILE: &str = "/home/ubuntu/rqservice/test_files/input_test_file.jpg"; // Path to a real sample file
        const STATIC_TEST_FILE: &str = "/home/ubuntu/rqservice/test_files/cp_detector.7z"; // Path to a real sample file


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
            #[serde(rename = "grpc-service")]
            grpc_service: String,
            #[serde(rename = "symbol-size")]
            symbol_size: u16,
            #[serde(rename = "redundancy-factor")]
            redundancy_factor: u8,
        }

        #[test]
        #[serial]
        fn test_rqprocessor() -> Result<(), Box<dyn std::error::Error>> {
                    
            let start_time = Instant::now(); // Mark the start time
            initialize_database(TEST_DB_PATH).unwrap();

            setup();
            // Read the configuration
            log::info!("Reading rqconfig.toml at path: rqconfig.toml");
            let config = toml::from_str::<Config>(&fs::read_to_string(RQ_CONFIG_PATH)?)?;

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
            let processor = RaptorQProcessor::new(TEST_DB_PATH).unwrap();                        
            log::info!("RaptorQProcessor created.");

            // Compute original file hash
            let original_file_hash = sha3_256_hash(&input_test_file_data);
            log::info!("Original file hash computed: {}", original_file_hash);

            // Create metadata and store
            log::info!("Creating metadata and storing...");
            let pool = setup_pool();
            log::info!("Pool created.");
            let (metadata, _db_path) = processor.create_metadata_and_store(
                &input_test_file,
                &"block_hash".to_string(),
                &"pastel_id".to_string(),
                &pool,
            )?;
            log::info!("Metadata created and stored.");

            // Attempt to create metadata and store again for the same file
            log::info!("Attempting to create metadata and store again...");
            let result = processor.create_metadata_and_store(
                &input_test_file,
                &"block_hash".to_string(),
                &"pastel_id".to_string(),
                &pool,
            );

            // Verify that the correct error message is returned
            match result {
                Err(RqProcessorError { func, msg, prev_msg: _ }) if func == "create_metadata_and_store" && msg.starts_with("Original file already exists") => {
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
            let decoded_file_path = processor.decode(&conn, &metadata.encoder_parameters, &original_file_hash)?;
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
            // fs::remove_file(TEST_DB_PATH)?;
            log::info!("Clean up complete!");
            let elapsed_time = start_time.elapsed();  // Calculate the elapsed time
            log::info!("End-to-End Test total execution time: {:?}", elapsed_time);
            Ok(())
        }            
}
}

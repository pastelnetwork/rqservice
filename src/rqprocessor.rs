// Copyright (c) 2021-2023 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};
use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use std::{fmt, fs, io};
use chrono::Utc;
use serde_derive::{Deserialize, Serialize};
use rusqlite::{Connection, params};
use rayon::prelude::*;
use std::sync::Mutex;
use lazy_static::lazy_static;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use toml;

lazy_static! {
    static ref WRITE_LOCK: Mutex<()> = Mutex::new(());
}

pub const NUM_WORKERS: usize = 4;
pub const DB_PATH: &str = "/home/ubuntu/.pastel/testnet3/rq_symbols.sqlite";

#[derive(Deserialize)]
struct RqConfig {
    _grpc_service: String,
    symbol_size: u16,
    redundancy_factor: u8,
}

fn read_config() -> Result<RqConfig, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string("rqconfig.toml")?;
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
}



pub struct RaptorQProcessor {
    symbol_size: u16,
    redundancy_factor: u8,
}

impl RaptorQProcessor {

    pub fn create_processor() -> Result<RaptorQProcessor, Box<dyn std::error::Error>> {
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
    
    /// Initializes the SQLite database with the given path
    pub fn initialize_db(path: &str) -> Result<Connection, rusqlite::Error> {
        let conn = Connection::open(path)?;

        let desired_page_size = 65536;
    
        // Check the current page size
        let current_page_size: i32 = conn.query_row("PRAGMA page_size;", params![], |row| row.get(0))?;
    
        // If the page size is different from the desired value, set it and execute VACUUM
        if current_page_size != desired_page_size {
            conn.execute_batch(&format!("PRAGMA page_size = {};", desired_page_size))?;
            conn.execute_batch("VACUUM;")?;
        }
    
        // Set other PRAGMAs only if they are different from the desired values
        macro_rules! set_pragma_if_different {
            ($pragma:expr, $value:expr) => {
                let current_value: String = conn.query_row(&format!("PRAGMA {};", $pragma), params![], |row| row.get(0))?;
                if current_value != $value {
                    conn.execute_batch(&format!("PRAGMA {} = {};", $pragma, $value))?;
                }
            };
        }
    
        set_pragma_if_different!("journal_mode", "WAL");
        set_pragma_if_different!("synchronous", "NORMAL");
        set_pragma_if_different!("cache_size", "-524288");
        set_pragma_if_different!("busy_timeout", "3000");
        set_pragma_if_different!("wal_autocheckpoint", "100");
    
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
                pastel_id TEXT,

            )",
            params![],
        )?;
        Ok(conn)
    }

    fn insert_symbol(tx: &rusqlite::Transaction, symbol_hash: &str, original_file_hash: &str, symbol_data: &[u8], timestamp: i64) -> Result<(), rusqlite::Error> {
        log::info!("Inserting symbol with hash: {}", symbol_hash);
        tx.execute(
            "INSERT INTO rq_symbols (original_file_sha3_256_hash, rq_symbol_file_sha3_256_hash, rq_symbol_file_data, utc_datetime_symbol_file_created) VALUES (?1, ?2, ?3, ?4)",
            params![original_file_hash, symbol_hash, symbol_data, timestamp],
        )?;
        log::info!("Inserted symbol with hash: {}", symbol_hash);
        Ok(())
    }

    fn insert_original_file(tx: &rusqlite::Transaction, original_file_hash: &str, original_file_path: &str, original_file_size_in_mb: f64, files_number: i32, block_hash: &str, pastel_id: &str, encoder_parameters: &[u8]) -> Result<(), rusqlite::Error> {
        log::info!("Inserting original file with hash: {}", original_file_hash);
        tx.execute(
            "INSERT OR REPLACE INTO original_files (original_file_sha3_256_hash, original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![original_file_hash, original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters],
        )?;
        log::info!("Inserted original file with hash: {}", original_file_hash);
        Ok(())
    }

    // fn insert_worker(
    //     &self,
    //     rx_queue: crossbeam::channel::Receiver<WriteOperation>,
    //     mut conn: r2d2::PooledConnection<SqliteConnectionManager>,
    // ) -> Result<(), Box<dyn std::error::Error>> {
    //     let tx = conn.transaction()?;
    
    //     // Process the write operations from the queue
    //     for write_op in rx_queue.iter() {
    //         match write_op {
    //             WriteOperation::OriginalFile((original_file_hash, original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters)) => {
    //                 Self::insert_original_file(&tx, &original_file_hash, &original_file_path, original_file_size_in_mb, files_number, &block_hash, &pastel_id, &encoder_parameters)?;
    //                 log::info!("Inserted original file metadata for hash: {}", original_file_hash);
    //             },
    //             WriteOperation::Symbol((original_file_hash, symbol_hash, symbol_data, timestamp)) => {
    //                 Self::insert_symbol(&tx, &symbol_hash, &original_file_hash, &symbol_data, timestamp)?;
    //                 log::info!("Inserted symbol for original file hash: {}", original_file_hash);
    //             },
    //         }
    //     }
    
    //     tx.commit()?;
    //     Ok(())
    // }
    
    fn insert_worker_func(
        rx_queue: crossbeam::channel::Receiver<WriteOperation>,
        mut conn: r2d2::PooledConnection<SqliteConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tx: rusqlite::Transaction<'_> = conn.transaction()?;
    
        // Process the write operations from the queue
        for write_op in rx_queue.iter() {
            match write_op {
                WriteOperation::OriginalFile((original_file_hash, original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters)) => {
                    RaptorQProcessor::insert_original_file(&tx, &original_file_hash, &original_file_path, original_file_size_in_mb, files_number, &block_hash, &pastel_id, &encoder_parameters)?;
                    log::info!("Inserted original file metadata for hash: {}", original_file_hash);
                },
                WriteOperation::Symbol((original_file_hash, symbol_hash, symbol_data, timestamp)) => {
                    RaptorQProcessor::insert_symbol(&tx, &symbol_hash, &original_file_hash, &symbol_data, timestamp)?;
                    log::info!("Inserted symbol for original file hash: {}", original_file_hash);
                },
            }
        }
    
        tx.commit()?;
        Ok(())
    }
    
    fn retrieve_original_file(conn: &Connection, original_file_hash: &str) -> Result<(String, f64, i32, Vec<u8>), rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters FROM original_files WHERE original_file_sha3_256_hash = ?1")?;
        let mut rows = stmt.query(params![original_file_hash])?;
        if let Some(row) = rows.next()? {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        } else {
            Err(rusqlite::Error::QueryReturnedNoRows)
        }
    }

    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let config = read_config()?;
        Ok(RaptorQProcessor {
            symbol_size: config.symbol_size,
            redundancy_factor: config.redundancy_factor,
        })
    }

    pub fn create_metadata_and_store(
        &self,
        path: &String,
        block_hash: &String,
        pastel_id: &String,
        pool: &Pool<SqliteConnectionManager>
    ) -> Result<(EncoderMetaData, String), RqProcessorError> {
        let input = Path::new(&path);
        let original_file_hash = self.compute_original_file_hash(&input)?;
        let (enc, repair_symbols) = self.get_encoder(input)?;
        let names_len = enc.get_encoded_packets(repair_symbols).len() as u32;
    
        // Prepare original file metadata
        let original_file_metadata = (
            original_file_hash.clone(),
            path.clone(),
            input.metadata().ok().map_or(0.0, |m| m.len() as f64 / 1_000_000.0),
            names_len as i32,
            block_hash.clone(),
            pastel_id.clone(),
            enc.get_config().serialize().to_vec(),
        );
    
        // Prepare symbols
        let symbols_and_files: Vec<_> = 
            enc.get_encoded_packets(repair_symbols)
            .par_iter()
            .map(|packet| (original_file_hash.clone(), RaptorQProcessor::symbols_id(&packet.serialize()), packet.serialize(), get_current_timestamp()))
            .collect();
    
        let (tx_queue, rx_queue) = crossbeam::channel::unbounded();
        let workers: Vec<_> = (0..NUM_WORKERS)
            .map(|_| {
                let rx_queue = rx_queue.clone();
                let pool = pool.clone(); // Clone the pool here
                std::thread::spawn(move || {
                    let conn: r2d2::PooledConnection<SqliteConnectionManager> = pool.get().expect("Failed to get connection from pool.");
                    RaptorQProcessor::insert_worker_func(rx_queue, conn).expect("Insert worker failed");
                })
            })
            .collect();
            
        // Send original file metadata to the worker
        tx_queue.send(WriteOperation::OriginalFile(original_file_metadata)).unwrap();
    
        // Send symbols to the worker
        for symbol_data in symbols_and_files {
            tx_queue.send(WriteOperation::Symbol(symbol_data)).unwrap();
        }
    
        // Wait for worker threads to complete
        for worker in workers {
            worker.join().expect("Worker thread panicked");
        }
    
        Ok(
            (EncoderMetaData {
                encoder_parameters: enc.get_config().serialize().to_vec(),
                source_symbols: names_len - repair_symbols,
                repair_symbols},
            original_file_hash) // original_file_hash remains accessible here
        )
    }

    pub fn encode(&self, path: &String, pool: &Pool<SqliteConnectionManager>) -> Result<(EncoderMetaData, String), RqProcessorError> {
        log::info!("Starting encoding process for file: {}", path);
        let input = Path::new(&path);
        let (enc, repair_symbols) = self.get_encoder(input)?;
        log::info!("Encoder obtained with {} repair symbols.", repair_symbols);
    
        let original_file_hash = self.compute_original_file_hash(&input)?;
        log::info!("Original file hash computed: {}", original_file_hash);
    
        let (tx_queue, rx_queue) = crossbeam::channel::unbounded();
        
        let encoded_symbols = enc.get_encoded_packets(repair_symbols);            
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
    
        // Wait for worker threads to complete
        for worker in workers {
            worker.join().expect("Worker thread panicked")?;
        }
                        
        log::info!("Symbols inserted successfully.");
    
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
                
        let output_path = format!("{}_decoded", original_file_path);
        let output_path_as_path = Path::new(&output_path); // Convert the string to a Path
        let mut file = File::create(&output_path_as_path)
            .map_err(|err| RqProcessorError::new_file_err("decode", "Cannot create output file", &output_path_as_path, err.to_string()))?;
        file.write_all(&result)?;
    
        Ok(output_path)
    }

    pub fn decode_with_selected_symbols(&self, selected_symbols: &[Vec<u8>], encoder_parameters: &Vec<u8>) -> Result<Vec<u8>, RqProcessorError> {
        if encoder_parameters.len() != 12 {
            return Err(RqProcessorError::new("decode_with_selected_symbols", "encoder_parameters length must be 12", "".to_string()));
        }
    
        let mut cfg = [0u8; 12];
        cfg.copy_from_slice(encoder_parameters);
        let config = ObjectTransmissionInformation::deserialize(&cfg);
        let mut dec = Decoder::new(config);
    
        // Deserialize symbols into EncodingPacket objects and add them to the decoder
        for symbol_data in selected_symbols {
            let symbol_packet = EncodingPacket::deserialize(&symbol_data);
            dec.add_new_packet(symbol_packet);
        }
    
        // Retrieve the result
        let result = dec.get_result().ok_or_else(|| RqProcessorError::new("decode_with_selected_symbols", "Decoding failed", "".to_string()))?;
    
        Ok(result)
    }

    fn get_encoder(&self, path: &Path) -> Result<(Encoder, u32), RqProcessorError> {

        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(err) => {
                return Err(RqProcessorError::new_file_err("get_encoder",
                    "Cannot open file",
                    path,
                    err.to_string()));
            }
        };

        let source_size = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(err) => {
                return Err(RqProcessorError::new_file_err("get_encoder",
                    "Cannot access metadata of file",
                    path,
                    err.to_string()));
            }
        };

        let config = ObjectTransmissionInformation::with_defaults(
            source_size,
            self.symbol_size,
        );

        let mut data = Vec::new();
        match file.read_to_end(&mut data) {
            Ok(_) => Ok((Encoder::new(&data, config),
                RaptorQProcessor::repair_symbols_num(self.symbol_size,
                    self.redundancy_factor,
                    source_size))),
            Err(err) => {
                Err(RqProcessorError::new_file_err("get_encoder",
                    "Cannot read input file",
                    path,
                    err.to_string()))
            }
        }
    }

    fn repair_symbols_num(symbol_size: u16, redundancy_factor: u8, data_len: u64) -> u32 {
        if data_len <= symbol_size as u64 {
            redundancy_factor as u32
        } else {
            (data_len as f64 *
                (f64::from(redundancy_factor) - 1.0) /
                f64::from(symbol_size)).ceil() as u32
        }
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

    // Utility Functions
    fn setup_pool() -> Pool<SqliteConnectionManager> {
        let manager = SqliteConnectionManager::file(DB_PATH); // Assuming DB_PATH is the path to your SQLite database
        Pool::new(manager).expect("Failed to create pool.")
    }

    fn test_meta(pool: &Pool<SqliteConnectionManager>, path: String, size: u32) -> Result<(EncoderMetaData, String), TestError> {
        log::info!("Testing file {}", path);
        let processor = RaptorQProcessor::new().map_err(TestError::Other)?;

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
        let processor = RaptorQProcessor::new().map_err(TestError::Other)?; // Updated constructor

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
        let processor = RaptorQProcessor::new().map_err(TestError::Other)?; // Updated constructor

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

    #[test]
    fn rq_test_metadata() -> Result<(), TestError> {
        let pool = setup_pool();
        test_meta(&pool, String::from("test/10_000_000"), 10_000_000)?;
        Ok(())
    }
    

    #[test]
    fn rq_test_encode_decode() -> Result<(), TestError> {
        let pool = setup_pool(); // Assuming setup_pool returns the connection pool
        let path = String::from("test/10_000_000");
        let (meta, _path) = test_encode(&pool, path.clone(), 10_000_000)?;
    
        // Compute the original file hash
        let input = Path::new(&path);
        let processor = RaptorQProcessor::new().map_err(TestError::Other)?; // Updated constructor
        let original_file_hash = processor.compute_original_file_hash(&input).expect("Failed to compute original file hash");
    
        test_decode(&pool, &meta.encoder_parameters, &original_file_hash)?;
    
        Ok(())
    }


    #[test]
    fn test_compute_original_file_hash() {
        use std::fs::File;
        use std::io::Write;
        use rand::Rng;
        use tempfile::tempdir;
    
        // Sizes to test: one that's a multiple of the chunk size and one that's not
        let sizes_to_test = [256 * 1024, 256 * 1024 + 1];
    
        let processor = RaptorQProcessor::new().expect("Failed to create processor"); // Updated constructor
    
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

        const TEST_DB_PATH: &str = "test_rq_symbols.sqlite";
        const STATIC_TEST_FILE: &str = "input_test_file.jpg"; // Path to a real sample file


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
        _grpc_service: String,
        _symbol_size: usize,
        redundancy_factor: usize,
    }

        #[test]
        fn test_rqprocessor() -> Result<(), Box<dyn std::error::Error>> {
            // Read the configuration
            let config = toml::from_str::<Config>(&fs::read_to_string("rqconfig.toml")?)?;
            let redundancy_factor = config.redundancy_factor as f64;

            // Choose between using a static test file or generating a random test file
            let (input_test_file, input_test_file_data) = if Path::new(STATIC_TEST_FILE).exists() {
                (STATIC_TEST_FILE.to_string(), fs::read(STATIC_TEST_FILE)?)
            } else {
                generate_test_file()?
            };

            // Initialize database
            let conn = RaptorQProcessor::initialize_db(TEST_DB_PATH)?;

            // Create processor
            let processor = RaptorQProcessor::create_processor()?;

            // Compute original file hash
            let original_file_hash = sha3_256_hash(&input_test_file_data);

            // Create metadata and store
            let pool = setup_pool();
            let (metadata, _db_path) = processor.create_metadata_and_store(
                &input_test_file,
                &"block_hash".to_string(),
                &"pastel_id".to_string(),
                &pool,
            )?;

            // Number of random decoding attempts
            let attempts = 10;
            // Number of symbols to fetch for decoding
            let symbols_to_fetch = ((1.0 / redundancy_factor) * 1.05 * metadata.source_symbols as f64).ceil() as usize;


            for _ in 0..attempts {
                // Prepare the statement
                let mut stmt = conn.prepare("SELECT rq_symbol_file_data FROM rq_symbols WHERE original_file_sha3_256_hash = ?1")?;
                // Query the symbols with the original file hash
                let all_symbols: Result<Vec<Vec<u8>>, _> = stmt.query_map(params![&original_file_hash], |row| {
                    row.get(0)
                })?.collect();
        
                // Unwrap the result or handle the error as needed
                let all_symbols = all_symbols?;
        
                // Randomly select a subset of symbols
                let mut rng = rand::thread_rng();
                let selected_symbols: Vec<Vec<u8>> = all_symbols.choose_multiple(&mut rng, symbols_to_fetch).cloned().collect();
        
                // Decode using the selected subset and verify
                let decoded_file_data = processor.decode_with_selected_symbols(&selected_symbols, &metadata.encoder_parameters)?;
                let decoded_file_hash = sha3_256_hash(&decoded_file_data);
                assert_eq!(original_file_hash, decoded_file_hash, "Reconstructed file hash does not match original file hash");
            }
        
            // Clean up (optional)
            if !Path::new(STATIC_TEST_FILE).exists() {
                fs::remove_file(input_test_file)?;
            }
            fs::remove_file(TEST_DB_PATH)?;
        
            Ok(())
        }
}
}

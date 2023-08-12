// Copyright (c) 2021-2023 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};

use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;
use std::fs::File;
use std::{fs, fmt, io};
// use log::{info, error};
use serde_derive::{Deserialize, Serialize};
use rusqlite::{Connection, params};
use rayon::prelude::*;
use std::sync::Mutex;
use lazy_static::lazy_static;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use crossbeam::channel::Receiver;
use r2d2::PooledConnection;

lazy_static! {
    static ref WRITE_LOCK: Mutex<()> = Mutex::new(());
}

pub const NUM_WORKERS: usize = 4;
pub const DB_PATH: &str = "/home/ubuntu/.pastel/testnet3/rq_symbols.sqlite";

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

fn insert_worker(rx_queue: Receiver<(String, Vec<u8>)>, original_file_hash: String, mut conn: PooledConnection<SqliteConnectionManager>) -> Result<(), RqProcessorError> {
    let tx = conn.transaction()?;
    let timestamp = std::time::SystemTime::now()
    .duration_since(std::time::SystemTime::UNIX_EPOCH)
    .expect("Time went backwards")
    .as_secs() as i64;

    for (name, pkt) in rx_queue.iter() {
        RaptorQProcessor::insert_symbol(&tx, &name, &original_file_hash, &pkt, timestamp)
            .map_err(|e| RqProcessorError::from(e.to_string()))?;
    }

    tx.commit()?;
    Ok(())
}


impl RaptorQProcessor {

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
    
    fn perform_transaction<F>(conn: &mut Connection, operation: F) -> Result<(), rusqlite::Error>
    where
        F: FnOnce(&rusqlite::Transaction) -> Result<(), rusqlite::Error>,
    {
        let _guard = WRITE_LOCK.lock().unwrap();
        let tx = conn.transaction()?;
        operation(&tx)?;
        tx.commit()
    }


    fn insert_symbol(tx: &rusqlite::Transaction, symbol_hash: &str, original_file_hash: &str, symbol_data: &[u8], timestamp: i64) -> Result<(), rusqlite::Error> {
        tx.execute(
            "INSERT INTO rq_symbols (original_file_sha3_256_hash, rq_symbol_file_sha3_256_hash, rq_symbol_file_data, utc_datetime_symbol_file_created) VALUES (?1, ?2, ?3, ?4)",
            params![original_file_hash, symbol_hash, symbol_data, timestamp],
        )?;
        Ok(())
    }

    fn insert_symbols(
        &self,
        conn: &mut Connection,
        symbols_and_files: &[(String, String, Vec<u8>, i64)],
    ) -> Result<(), rusqlite::Error>{
        Self::perform_transaction(conn, |tx| {
            for &(ref symbol_hash, ref original_file_hash, ref symbol_data, timestamp) in symbols_and_files {
                Self::insert_symbol(tx, symbol_hash, original_file_hash, symbol_data, timestamp)?;
            }
            Ok(())
        })
    }

    fn insert_original_file(conn: &mut Connection, original_file_hash: &str, original_file_path: &str, original_file_size_in_mb: f64, files_number: i32, block_hash: &str,  pastel_id: &str, encoder_parameters: &[u8]) -> Result<(), rusqlite::Error> {
        let _guard = WRITE_LOCK.lock().unwrap();
        let tx = conn.transaction()?;
        
        tx.execute(
            "INSERT OR REPLACE INTO original_files (original_file_sha3_256_hash, original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![original_file_hash, original_file_path, original_file_size_in_mb, files_number, block_hash, pastel_id, encoder_parameters],
        )?;
        tx.commit()
    }

    fn retrieve_symbol(conn: &Connection, symbol_hash: &str) -> Result<Vec<u8>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT rq_symbol_file_data FROM rq_symbols WHERE rq_symbol_file_sha3_256_hash = ?1")?;
        let mut rows = stmt.query(params![symbol_hash])?;
        if let Some(row) = rows.next()? {
            Ok(row.get(0)?)
        } else {
            Err(rusqlite::Error::QueryReturnedNoRows)
        }
    }
    
    fn retrieve_symbols(&self, pool: &Pool<SqliteConnectionManager>, symbol_hashes: &[&str]) -> Result<Vec<Vec<u8>>, RqProcessorError> {
        symbol_hashes.par_iter()
            .map(|&symbol_hash| {
                let conn = pool.get().map_err(RqProcessorError::from)?;
                RaptorQProcessor::retrieve_symbol(&conn, symbol_hash).map_err(RqProcessorError::from)
            })
            .collect()
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
    
    fn transaction<F>(conn: &mut Connection, f: F) -> Result<(), rusqlite::Error>
    where
        F: FnOnce() -> Result<(), rusqlite::Error>,
    {
        let _guard = WRITE_LOCK.lock().unwrap();
        let tx = conn.transaction()?;
        f()?;
        tx.commit()
    }
    
    

    pub fn new(symbol_size: u16, redundancy_factor: u8) -> Self {

        RaptorQProcessor {
            symbol_size,
            redundancy_factor,
        }
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
        let original_file_hash_clone = original_file_hash.clone(); // Clone the original_file_hash value
        
        let (enc, repair_symbols) = self.get_encoder(input)?;
        let names_len = enc.get_encoded_packets(repair_symbols).len() as u32;

        let mut conn = pool.get().expect("Failed to get connection from pool.");
        
        // Store metadata in the `original_files` table
        Self::insert_original_file(
            &mut conn,
            &original_file_hash,
            path,
            input.metadata().ok().map_or(0.0, |m| m.len() as f64 / 1_000_000.0),
            names_len as i32,
            block_hash,
            pastel_id,
            &enc.get_config().serialize().to_vec(),
        )?;

        // Prepare data for `rq_symbols` table
        let symbols_and_files: Vec<_> = 
            enc.get_encoded_packets(repair_symbols)
            .par_iter()
            .map(|packet| (RaptorQProcessor::symbols_id(&packet.serialize()), packet.serialize()))
            .collect();

        let (tx_queue, rx_queue) = crossbeam::channel::unbounded();
        let workers: Vec<_> = (0..NUM_WORKERS)
        .map(|_| {
            let rx_queue = rx_queue.clone();
            let original_file_hash = original_file_hash_clone.clone(); // Use the cloned value here
            let conn = pool.get().expect("Failed to get connection from pool.");
            std::thread::spawn(move || {
                insert_worker(rx_queue, original_file_hash, conn).expect("Insert worker failed");
            })
        })
        .collect();

        for (name, symbol_data) in symbols_and_files {
            tx_queue.send((name, symbol_data)).unwrap();
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
        // Launch worker threads to handle the insertion of symbols
        let workers: Vec<_> = (0..NUM_WORKERS)
        .map(|_| {
            let rx_queue = rx_queue.clone();
            let original_file_hash = original_file_hash.clone(); // Clone the original_file_hash for each worker
            let conn = pool.get().expect("Failed to get connection from pool.");
            std::thread::spawn(move || {
                insert_worker(rx_queue, original_file_hash, conn).expect("Insert worker failed");
            })
        })
        .collect();
    
        let original_file_hash = self.compute_original_file_hash(&input)?;
        log::info!("Original file hash computed: {}", original_file_hash);
    
        let encoded_symbols = enc.get_encoded_packets(repair_symbols);            
        log::info!("Symbols obtained for encoding.");            
    
        // Send symbols to worker threads for insertion
        encoded_symbols.par_iter()
            .map(|symbol| {
                let pkt = symbol.serialize();
                let name = RaptorQProcessor::symbols_id(&pkt);
                tx_queue.send((name, pkt)).unwrap();
            })
            .collect::<Vec<_>>();
    
        // Wait for worker threads to complete
        for worker in workers {
            worker.join().expect("Worker thread panicked");
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
    

    fn output_location(input: &Path, sub: &str)
        -> Result<(String, PathBuf), RqProcessorError> {

        let output_path = match input.parent() {
            Some(p) => {
                if !sub.is_empty() {
                    p.join(sub)
                } else {
                    p.to_path_buf()
                }
            },
            None => {
                return Err(RqProcessorError::new_file_err("output_location",
                    "Cannot get parent of the input location",
                    input,
                    "".to_string()));
            }
        };
        if let Err(err) = fs::create_dir_all(&output_path) {
            return Err(RqProcessorError::new_file_err("output_location",
                "Cannot create output location",
                output_path.as_path(),
                err.to_string()));
        }

        match RaptorQProcessor::path_buf_to_string(&output_path, "output_location", "Invalid path") {
            Ok(path_str) => Ok((path_str.to_string(), output_path)),
            Err(err) => Err(err)
        }
    }

    fn path_buf_to_string(path: &PathBuf, func: &str, msg: &str) -> Result<String, RqProcessorError> {
        match path.to_str() {
            Some(path_str) => Ok(path_str.to_string()),
            None => Err(RqProcessorError::new_file_err(func,
                msg,
                path.as_path(),
                "".to_string()))
        }
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

    fn create_and_write<F>(func: &str, output_file_path: &PathBuf, f: F)
        -> Result<(), RqProcessorError>
        where F: Fn(File) -> std::io::Result<()> {

        let output_file = match File::create(&output_file_path) {
            Ok(file) => file,
            Err(err) => {
                return Err(RqProcessorError::new_file_err(func,
                    "Cannot create file",
                    output_file_path.as_path(),
                    err.to_string()));
            }
        };

        if let Err(err) = f(output_file) {
            return Err(RqProcessorError::new_file_err(func,
                "Cannot write into the file",
                output_file_path.as_path(),
                err.to_string()));
        };
        Ok(())
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



#[derive(Debug, Clone)]
pub struct RaptorQProcessor {
    symbol_size: u16,
    redundancy_factor: u8,
}

#[derive(Debug, Clone)]
pub struct EncoderMetaData {
    pub encoder_parameters: Vec<u8>,
    pub source_symbols: u32,
    pub repair_symbols: u32
}

#[derive(Debug, Clone)]
pub struct RqProcessorError {
    func: String,
    msg: String,
    prev_msg: String
}

#[derive(Serialize, Deserialize)]
struct RqIdsFile {
    id: String,
    block_hash: String,
    pastel_id: String,
    symbol_identifiers: Vec<String>
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


/*
To run tests generate 3 random files first inside test directory:
$ dd if=/dev/urandom of=10_000 bs=1 count=10000
$ dd if=/dev/urandom of=10_000_000 bs=1 count=10000000
$ dd if=/dev/urandom of=10_000_001 bs=1 count=10000001
*/

#[derive(Debug)]
enum TestError {
    MetaError(String),
    EncodeError(String),
    DecodeError(String),
    RqProcessorError(RqProcessorError),
    DbError(rusqlite::Error),
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use r2d2::Pool;
    use r2d2_sqlite::SqliteConnectionManager;

    fn test_meta(pool: &Pool<SqliteConnectionManager>, path: String, size: u32) -> Result<(EncoderMetaData, String), TestError> {
        log::info!("Testing file {}", path);
        let processor = RaptorQProcessor::new(50_000, 12);
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
        let processor = RaptorQProcessor::new(50_000, 12);
    
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
        let processor = RaptorQProcessor::new(50_000, 12);

        match processor.decode(&conn, encoder_parameters, original_file_hash) {
            Ok(output_path) => {
                log::info!("Restored file path: {}", output_path);
                Ok(())
            },
            Err(e) => Err(TestError::DecodeError(format!("decode returned Error - {:?}", e))),
        }
}

fn setup_pool() -> Pool<SqliteConnectionManager> {
    let manager = SqliteConnectionManager::file(DB_PATH); // Assuming DB_PATH is the path to your SQLite database
    Pool::new(manager).expect("Failed to create pool.")
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
    let (meta, _path) = test_encode(&pool, path.clone(), 10_000_000).unwrap();

    // Compute the original file hash
    let input = Path::new(&path);
    let processor = RaptorQProcessor::new(50_000, 12);
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

    let processor = RaptorQProcessor::new(50_000, 12);

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
}

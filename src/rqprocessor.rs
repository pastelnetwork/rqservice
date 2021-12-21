// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};
use itertools::{Itertools};

use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;
use std::fs::File;
use std::{fs, fmt, io};
use uuid::Uuid;
use serde_derive::{Deserialize, Serialize};

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

impl RaptorQProcessor {

    pub fn new(symbol_size: u16, redundancy_factor: u8) -> Self {

        RaptorQProcessor {
            symbol_size,
            redundancy_factor,
        }
    }

    pub fn create_metadata(&self, path: &String, files_number: u32,
                           block_hash: &String, pastel_id: &String )
        -> Result<(EncoderMetaData, String), RqProcessorError> {

        let input = Path::new(&path);
        let (enc, repair_symbols) = self.get_encoder(input)?;

        let names : Vec<String> =
            enc.get_encoded_packets(repair_symbols)
            .iter()
            .map(|packet|
                {
                    RaptorQProcessor::symbols_id(&packet.serialize())
                }
            ).collect();

        let names_len = names.len() as u32;

        let mut rq_ids_file = RqIdsFile {
            id: "".to_string(),
            block_hash: block_hash.to_string(),
            pastel_id: pastel_id.to_string(),
            symbol_identifiers: names
        };


        let (output_path_str, output_path) =
            RaptorQProcessor::output_location(input, "meta")?;

        for _n in 0..files_number {
            let guid = Uuid::new_v4();
            let output_file_path = output_path.join(guid.to_string());

            rq_ids_file.id = guid.to_string();
            let j = serde_json::to_string(&rq_ids_file)?;

            RaptorQProcessor::create_and_write("create_metadata", &output_file_path,
                             |output_file| {
                                 write!(&output_file, "{}", j)
                             })?;

        }

        Ok(
            (EncoderMetaData {
                encoder_parameters: enc.get_config().serialize().to_vec(),
                source_symbols: names_len - repair_symbols,
                repair_symbols},
            output_path_str)
        )
    }

    pub fn encode(&self, path: &String) -> Result<(EncoderMetaData, String), RqProcessorError> {

        let input = Path::new(&path);
        let (enc, repair_symbols) = self.get_encoder(input)?;

        let (output_path_str, output_path) =
            RaptorQProcessor::output_location(input, "symbols")?;

        let symbols = enc.get_encoded_packets(repair_symbols);
        for symbol in &symbols {
            let pkt = symbol.serialize();

            let name = RaptorQProcessor::symbols_id(&pkt);
            let output_file_path = output_path.join(name);

            RaptorQProcessor::create_and_write("encode", &output_file_path,
                                               |output_file| {
                                                   (&output_file).write_all(&pkt)
                                               })?;
        }

        Ok(
            (EncoderMetaData {
            encoder_parameters: enc.get_config().serialize().to_vec(),
            source_symbols: symbols.len() as u32 - repair_symbols,
            repair_symbols},
            output_path_str
            )
        )
    }

    pub fn decode(self, encoder_parameters: &Vec<u8>, path: &String)
        -> Result<String, RqProcessorError> {

        if path.is_empty() {
            return Err(RqProcessorError::new("decode",
                                             "Input symbol's path is empty",
                                             "".to_string()));
        }
        if encoder_parameters.len() == 0 {
            return Err(RqProcessorError::new("decode",
                                             "encoder_parameters are empty",
                                             "".to_string()));
        }

        let mut cfg = [0u8; 12];
        cfg.iter_mut().set_from(encoder_parameters.iter().cloned());

        let config = ObjectTransmissionInformation::deserialize(&cfg);
        let mut dec = Decoder::new(config);

        let symbol_files = match fs::read_dir(&path) {
            Ok(paths) => paths,
            Err(err) => {
                return Err(RqProcessorError::new("decode",
                                                 format!("Cannot get list of input files from {}", &path).as_str(),
                                                 err.to_string()));
            }
        };

        for symbol_file in symbol_files {

            let file_path = match symbol_file {
                Ok(path) => path.path(),
                Err(err) => {
                    return Err(RqProcessorError::new("decode",
                                                     "Cannot get file path",
                                                     err.to_string()));
                }
            };

            let mut data = Vec::new();
            RaptorQProcessor::open_and_read("decode", &file_path, &mut data)?;

            if let Some(result) = dec.decode(EncodingPacket::deserialize(&data)) {

                let input = Path::new(&path);
                let rest_file = input.with_file_name("restored_file");
                let rest_file_str =
                    RaptorQProcessor::path_buf_to_string(
                        &rest_file, "decode", "Invalid path")?;

                RaptorQProcessor::create_and_write("decode", &rest_file,
                                                   |output_file| {
                                                       (&output_file).write_all(&result)
                                                   })?;
                return Ok(rest_file_str);
            };
        }

        Err(RqProcessorError::new("decode",
                                  format!("Cannot restore the original file from symbols at {}", path).as_str(),
                                  "".to_string()))
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
        if let Err(err) = fs::create_dir_all(&output_path)
        {
            return Err(RqProcessorError::new_file_err("output_location",
                                                      "Cannot create output location",
                                                      output_path.as_path(),
                                                      err.to_string()));
        }

        match RaptorQProcessor::path_buf_to_string(&output_path, "output_location", "Invalid path"){
            Ok(path_str) => Ok((path_str.to_string(), output_path)),
            Err(err) => Err(err)
        }

    }

    fn path_buf_to_string(path: &PathBuf, func: &str, msg: &str) -> Result<String, RqProcessorError> {
        match path.to_str(){
            Some(path_str) => Ok(path_str.to_string()),
            None => Err(RqProcessorError::new_file_err(func,
                                                       msg,
                                                       path.as_path(),
                                                       "".to_string()))
        }
    }

    fn get_encoder(&self, path: &Path) -> Result<(Encoder, u32), RqProcessorError> {

        let mut file = match File::open(&path){
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

        let mut data= Vec::new();
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

        let output_file = match File::create(&output_file_path){
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

    fn open_and_read(func: &str, file_path: &PathBuf, data: &mut Vec<u8>) -> Result<(), RqProcessorError> {

        let mut file = match File::open(&file_path) {
            Ok(file) => file,
            Err(err) => {
                return Err(RqProcessorError::new_file_err(func,
                                                          "Cannot open file",
                                                          file_path.as_path(),
                                                          err.to_string()));
            }
        };

        if let Err(err) = file.read_to_end(data) {
            return Err(RqProcessorError::new_file_err(func,
                                                      "Cannot read input file",
                                                      file_path.as_path(),
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
        bs58::encode(&hasher.finalize()).into_string()
    }
}

/*
To run tests generate 3 random files first inside test directory:
$ dd if=/dev/urandom of=10_000 bs=1 count=10000
$ dd if=/dev/urandom of=10_000_000 bs=1 count=10000000
$ dd if=/dev/urandom of=10_000_001 bs=1 count=10000001
*/

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn test_meta(path: String, size: u32) -> Option<(EncoderMetaData, String)> {
        println!("Testing file {}", path);

        let processor = RaptorQProcessor::new(
            50_000,
            12);

        let encode_time = Instant::now();
        match processor.create_metadata(&path, 50, &String::from("12345"), &String::from("67890")) {
            Ok((meta, path)) => {
                println!("source symbols = {}; repair symbols = {}", meta.source_symbols, meta.repair_symbols);

                let source_symbols = (size as f64 / 50_000.0f64).ceil() as u32;
                assert_eq!(meta.source_symbols, source_symbols);

                println!("{:?} spent to create symbols", encode_time.elapsed());
                Some((meta, path))
            },
            Err(e) => {
                assert!(false, "create_metadata returned Error - {:?}
                                NOTE: To run tests generate 3 random files irst inside test directory:
                                    $ dd if=/dev/urandom of=10_000 bs=1 count=10000
                                    $ dd if=/dev/urandom of=10_000_000 bs=1 count=10000000
                                    $ dd if=/dev/urandom of=10_000_001 bs=1 count=10000001", e);
                None
            }
        }
    }
    fn test_encode(path: String, size: u32) -> Option<(EncoderMetaData, String)> {
        println!("Testing file {}", path);

        let processor = RaptorQProcessor::new(
            50_000,
            12);

        let encode_time = Instant::now();
        match processor.encode(&path) {
            Ok((meta, path)) => {
                println!("source symbols = {}; repair symbols = {}", meta.source_symbols, meta.repair_symbols);

                let source_symbols = (size as f64 / 50_000.0f64).ceil() as u32;
                assert_eq!(meta.source_symbols, source_symbols);

                println!("{:?} spent to create symbols", encode_time.elapsed());
                Some((meta, path))
            },
            Err(e) => {
                assert!(false, "encode returned Error - {:?}
                                NOTE: To run tests generate 3 random files irst inside test directory:
                                    $ dd if=/dev/urandom of=10_000 bs=1 count=10000
                                    $ dd if=/dev/urandom of=10_000_000 bs=1 count=10000000
                                    $ dd if=/dev/urandom of=10_000_001 bs=1 count=10000001\n", e);
                None
            }
        }
    }

    fn test_decode(encoder_parameters: &Vec<u8>, path: &String) {
        println!("Testing file {}", path);

        let processor = RaptorQProcessor::new(
            50_000,
            12);

        let encode_time = Instant::now();

        match processor.decode(encoder_parameters, &path) {
            Ok(_outpat) => {
                // assert_eq!(symbols_count, (source_symbols + meta.repair_symbols) as usize);
            },
            Err(e) => {
                assert!(false, "decode returned Error - {:?}", e)
            }
        };
        println!("{:?} spent to restore file", encode_time.elapsed());
    }

    #[test]
    fn rq_test_metadata() {
        // test_meta(String::from("test/10_000"), 10_000);
        test_meta(String::from("test/10_000_000"), 10_000_000);
        // test_meta(String::from("test/10_000_001"), 10_000_001);
    }

    #[test]
    fn rq_test_encode_decode() {
        let (meta, _path) = test_encode(String::from("test/10_000_000"), 10_000_000).unwrap();
        test_decode(&meta.encoder_parameters, &"test/symbols".to_string());

        // test_encode(String::from("test/10_000"), 10_000);
        // test_encode(String::from("test/10_000_001"), 10_000_001);
    }
}

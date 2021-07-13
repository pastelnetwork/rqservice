// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{Decoder, Encoder, /*EncodingPacket,*/ ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};
// use itertools::Itertools;
// use std::time::Instant;

use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;
use std::fs::File;
use std::fs;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RaptorQProcessor {
    symbol_size: u16,
    redundancy_factor: u8,
}

#[derive(Debug, Clone)]
pub struct EncoderMetaData {
    pub encoder_parameters: Vec<u8>,
    pub path: String,
    pub symbol_names: Vec<String>,
    pub source_size: u64,
    pub repair_symbols: u32
}

#[derive(Debug, Clone)]
pub struct RaptorQDecoder {

    dec: Decoder,
}

impl RaptorQProcessor {

    pub fn new(symbol_size: u16, redundancy_factor: u8) -> Self {

        RaptorQProcessor {
            symbol_size,
            redundancy_factor,
        }
    }

    fn get_encoder(&self, path: &Path) -> Result<(Encoder, u32), Box<dyn std::error::Error>> {

        let mut file = File::open(&path)?;
        let source_size = file.metadata()?.len();

        let config = ObjectTransmissionInformation::with_defaults(
            source_size,
            self.symbol_size,
        );

        let mut data= Vec::new();
        file.read_to_end(&mut data)?;

        Ok(
            (
                Encoder::new(&data, config),
                RaptorQProcessor::repair_symbols_num(self.symbol_size,
                                                 self.redundancy_factor,
                                                 source_size)
            )
        )
    }

    fn get_output(input: &Path, sub: &str) -> Result<(String, PathBuf), Box<dyn std::error::Error>> {
        let output_path = input.parent().ok_or("Invalid path")?.join(sub);
        fs::create_dir_all(&output_path)?;
        let output_path_str = output_path.to_str().ok_or("Invalid path")?;

        Ok((output_path_str.to_string(), output_path))
    }

    fn get_symbols_id(symbol: &Vec<u8>) -> String {
        let mut hasher = Sha3_256::new();
        hasher.update(symbol);
        base64_url::encode(&hasher.finalize())
    }

    pub fn create_metadata(&self, path: String, files_number: u32,
                           block_hash: String, pastel_id: String ) -> Result<EncoderMetaData, Box<dyn std::error::Error>> {

        let input = Path::new(&path);
        let (enc, repair_symbols) = self.get_encoder(input)?;

        let names : Vec<String> = enc.get_encoded_packets(repair_symbols)
            .iter()
            .map(|packet|
                {
                    RaptorQProcessor::get_symbols_id(&packet.serialize())
                }
            ).collect();

        let (output_path_str, output_path) = RaptorQProcessor::get_output(input, "meta")?;

        for _n in 0..files_number {
            let guid = Uuid::new_v4();
            let output_file_path = output_path.join(guid.to_string());
            let mut output_file = File::create(&output_file_path)?;
            write!(output_file, "{}\n", guid)?;
            write!(output_file, "{}\n", block_hash)?;
            write!(output_file, "{}\n", pastel_id)?;
            write!(output_file, "{}", names.join("\n"))?;
        }

        Ok(EncoderMetaData {
                encoder_parameters: enc.get_config().serialize().to_vec(),
                path: output_path_str.to_string(),
                symbol_names: names,
                source_size: enc.get_config().transfer_length(),
                repair_symbols
            }
        )
    }

    pub fn encode(&self, path: String) -> Result<(String, u32), Box<dyn std::error::Error>> {

        let input = Path::new(&path);
        let (enc, repair_symbols) = self.get_encoder(input)?;

        let (output_path_str, output_path) = RaptorQProcessor::get_output(input, "symbols")?;

        let symbols = enc.get_encoded_packets(repair_symbols);
        for symbol in &symbols {
            let pkt = symbol.serialize();

            let name = RaptorQProcessor::get_symbols_id(&pkt);
            let mut output_file = File::create(&output_path.join(name))?;
            output_file.write_all(&pkt)?;
        }

        Ok((output_path_str, symbols.len() as u32))
    }

    // pub fn decode(&mut self, symbol: &Vec<u8>) -> String {
    //
    //     let mut cfg = [0u8; 12];
    //     cfg.iter_mut().set_from(enc_info.iter().cloned());
    //
    //     let config = ObjectTransmissionInformation::deserialize(&cfg);
    //
    //     RaptorQDecoder {
    //         dec: Decoder::new(config)
    //     }
    //
    //     self.dec.decode(EncodingPacket::deserialize(&symbol))
    // }

    pub fn repair_symbols_num(symbol_size: u16, redundancy_factor: u8, data_len: u64) -> u32 {
        if data_len <= symbol_size as u64 {
            redundancy_factor as u32
        } else {
            (data_len as f64 *
                (f64::from(redundancy_factor) - 1.0) /
                f64::from(symbol_size)).ceil() as u32
        }
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
    // use rand::seq::SliceRandom;
    // use rand::Rng;

    fn get_meta(path: String) {
        println!("Testing file {}", path);

        let processor = RaptorQProcessor::new(
            50_000,
            12);

        let encode_time = Instant::now();
        match processor.create_metadata(path, 50, String::from("12345"), String::from("67890")) {
            Ok(meta) => {
                let symbols_count = meta.symbol_names.len();
                let source_symbols = (meta.source_size as f64 / 50_000.0f64).ceil() as u32;
                println!("original data size = {}; \
                            all symbols = {}; (source symbols = {}; repair symbols = {})",
                         meta.source_size,
                         symbols_count,
                         source_symbols,
                         meta.repair_symbols);

                assert_eq!(symbols_count, (source_symbols + meta.repair_symbols) as usize);
            },
            Err(e) => {
                assert!(false, "create_metadata returned Error - {:?}
                                NOTE: To run tests generate 3 random files irst inside test directory:
                                    $ dd if=/dev/urandom of=10_000 bs=1 count=10000
                                    $ dd if=/dev/urandom of=10_000_000 bs=1 count=10000000
                                    $ dd if=/dev/urandom of=10_000_001 bs=1 count=10000001\n", e)
            }
        };
        println!("{:?} spent to create symbols", encode_time.elapsed());
    }
    fn encode(path: String) {
        println!("Testing file {}", path);

        let processor = RaptorQProcessor::new(
            50_000,
            12);

        let encode_time = Instant::now();
        match processor.encode(path) {
            Ok((_outpath, _num)) => {
                // assert_eq!(symbols_count, (source_symbols + meta.repair_symbols) as usize);
            },
            Err(e) => {
                assert!(false, "create_metadata returned Error - {:?}
                                NOTE: To run tests generate 3 random files irst inside test directory:
                                    $ dd if=/dev/urandom of=10_000 bs=1 count=10000
                                    $ dd if=/dev/urandom of=10_000_000 bs=1 count=10000000
                                    $ dd if=/dev/urandom of=10_000_001 bs=1 count=10000001\n", e)
            }
        };
        println!("{:?} spent to create symbols", encode_time.elapsed());
    }
        // let symbols_count = symbols.len();
        // symbols.shuffle(&mut rand::thread_rng());
        //
        // let decode_time = Instant::now();
        //     let mut data = vec![];
        //     let mut dec = RaptorQDecoder::new(enc.serialized_encoder_info());
        //     for s in symbols {
        //         if let Some(result) = dec.decode(&s) {
        //             data = result;
        //             break;
        //         }
        //     }
        // println!("{:?} spent to restore original data", decode_time.elapsed());
        //
        // let source_symbols = (v_size as f64 / 50_000.0f64).ceil() as u32;
        // let restore_symbols = RaptorQEncoder::repair_symbols_num(50_000, 12, v_size);
        // println!("original data size = {}; \
        //             all symbols = {}; (source symbols = {}; \
        //             restore symbols = {}) restored data size = {}",
        //          v_size,
        //          symbols_count,
        //          source_symbols,
        //          restore_symbols,
        //          data.len());
        //
        // assert_eq!(enc.encoder_info().transfer_length() as usize, v_size);
        // assert_eq!(data.len(), v_size);
        //
        // assert_eq!(symbols_count, (source_symbols+restore_symbols) as usize);

    #[test]
    fn rq_test_metadata() {
        // get_meta(String::from("test/10_000"));
        get_meta(String::from("test/10_000_000"));
        // get_meta(String::from("test/10_000_001"));
    }

    #[test]
    fn rq_test_encode() {
        // encode(String::from("test/10_000"));
        encode(String::from("test/10_000_000"));
        // encode(String::from("test/10_000_001"));
    }
}

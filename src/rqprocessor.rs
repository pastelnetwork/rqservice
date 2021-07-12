// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};
use base64;
use itertools::Itertools;
use std::time::Instant;

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

    pub fn create_metadata(&self,
                           path: String, files_number: u32,
                           block_hash: String, pastel_id: String ) -> Option<EncoderMetaData> {

        let len = 0;
        let data= Vec::new();

        let config = ObjectTransmissionInformation::with_defaults(
            len as u64,
            self.symbol_size,
        );
        let enc = Encoder::new(&data, config);

        let oti = enc.get_config().serialize().to_vec();
        let repair_symbols = RaptorQProcessor::repair_symbols_num(self.symbol_size,
                                                                  self.redundancy_factor,
                                                                  len);
        let names = enc.get_encoded_packets(repair_symbols)
            .iter()
            .map(|packet|
                {
                    let mut hasher = Sha3_256::new();
                    hasher.update(packet.serialize());
                    base64::encode(hasher.finalize())
                }
            ).collect();

        Some(EncoderMetaData {
                encoder_parameters: oti,
                path: String::new(),
                symbol_names: names
            }
        )
    }

    // pub fn encode(&self) -> String {
    //
    //     let get_symbols = Instant::now();
    //     let symbols = self.enc.get_encoded_packets(self.repair_symbols);
    //     println!("{:?} spent to create symbols", get_symbols.elapsed());
    //
    //     symbols.iter()
    //         .map( |packet|
    //             {
    //                 packet.serialize()
    //             }
    //         )
    //         .collect()
    // }
    //
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

    pub fn repair_symbols_num(symbol_size: u16, redundancy_factor: u8, data_len: usize) -> u32 {
        if data_len <= symbol_size as usize {
            redundancy_factor as u32
        } else {
            (data_len as f32 *
                (f32::from(redundancy_factor) - 1.0) /
                f32::from(symbol_size)).ceil() as u32
        }
    }
    //
    // pub fn encoder_info(&self) -> ObjectTransmissionInformation {
    //     self.enc.get_config()
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use rand::seq::SliceRandom;
    use rand::Rng;

    fn encode_decode(v_size: usize) {
        println!("Testing {} bytes array", v_size);

        // let make_array = Instant::now();
        //     let mut vec : Vec<u8> = vec![0; v_size];
        //     for i in 0..vec.len() {
        //         vec[i] = rand::thread_rng().gen();
        //     }
        // println!("{:?} spent to create array", make_array.elapsed());
        //
        // let encode_time = Instant::now();
        //     let enc = RaptorQEncoder::new(10240, 12, &vec);
        //     let mut symbols = enc.get_packets();
        // println!("{:?} spent to create symbols", encode_time.elapsed());
        //
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
    }

    #[test]
    fn rq_test_10_000_s() {
        encode_decode(10_000);
    }

    #[test]
    fn rq_test_10_000_000_s() {
        encode_decode(10_000_000);
    }

    #[test]
    fn rq_test_10_000_001_s() {
        encode_decode(10_000_001);
    }
}

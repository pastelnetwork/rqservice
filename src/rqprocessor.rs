// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};
use base64;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct RaptorQEncoder {

    symbol_size: u16,
    redundancy_factor: u8,
    repair_symbols: u32,
    enc: Encoder,
}

#[derive(Debug, Clone)]
pub struct RaptorQDecoder {

    dec: Decoder,
}

impl RaptorQEncoder {

    pub fn new(symbol_size: u16, redundancy_factor: u8, data: &Vec<u8>) -> Self {
        let repair_symbols = (dbg!(data.len()) as u32) *
            (u32::from(redundancy_factor)-1) /
                u32::from(symbol_size);

        let config = ObjectTransmissionInformation::with_defaults(
            data.len() as u64,
            symbol_size,
        );

        RaptorQEncoder {
            symbol_size,
            redundancy_factor,
            repair_symbols,
            enc: Encoder::new(data, config)
        }
    }

    pub fn get_names(&self) -> Vec<String> {

        self.enc.get_encoded_packets(self.repair_symbols)
            .iter()
            .map( |packet|
                {
                    let mut hasher = Sha3_256::new();
                    hasher.update(packet.serialize());
                    base64::encode(hasher.finalize())
                }
            ).collect()
    }

    pub fn get_packets(&self) -> Vec<Vec<u8>> {

        self.enc.get_encoded_packets(self.repair_symbols)
            .iter()
            .map( |packet|
                {
                    packet.serialize()
                }
            )
            .collect()
    }

    pub fn serialized_encoder_info(&self) -> Vec<u8> {
        self.enc.get_config().serialize().to_vec()
    }

    pub fn encoder_info(&self) -> ObjectTransmissionInformation {
        self.enc.get_config()
    }
}

impl RaptorQDecoder {
    pub fn new(enc_info: Vec<u8>) -> Self {

        let mut cfg = [0u8; 12];
        cfg.iter_mut().set_from(enc_info.iter().cloned());

        let config = ObjectTransmissionInformation::deserialize(&cfg);

        RaptorQDecoder {
            dec: Decoder::new(config)
        }
    }

    pub fn decode(&mut self, symbol: &Vec<u8>) -> Option<Vec<u8>> {

        self.dec.decode(EncodingPacket::deserialize(&symbol))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use rand::seq::SliceRandom;
    use rand::Rng;
    use std::time::Instant;

    fn simple_test(v_size: usize) {
        let make_array = Instant::now();
        let mut vec : Vec<u8> = vec![0; v_size];
        for i in 0..vec.len() {
            vec[i] = rand::thread_rng().gen();
        }
        println!("{:?} spent to create array", make_array.elapsed());

        let enc = RaptorQEncoder::new(50000, 12, &vec);

        let start = Instant::now();

        let symbols = enc.get_packets();

        println!("{:?} spent to create symbols' names", start.elapsed());

        let tr_length = dbg!(enc.encoder_info()).transfer_length();
        println!("symbols {}", symbols.len());

        assert_eq!(tr_length as usize, v_size);
        // assert_eq!(names.len(), );

        let mut data = vec![];
        let mut dec = RaptorQDecoder::new(enc.serialized_encoder_info());
        for s in symbols {
            match dec.decode(&s) {
                Some(done) =>  {data = done; break;},
                None => {}
            }
        }
        println!("data {}", data.len());

    }

    #[test]
    fn encode_data_10_000_s() {
        simple_test(10_000);
    }

    #[test]
    fn encode_data_10_000_000_s() {
        simple_test(10_000_000);
    }

    #[test]
    fn encode_data_10_000_001_s() {
        simple_test(10_000_001);
    }

    #[test]
    fn encode_data_100_000_001_s() {
        simple_test(100_000_001);
    }
}

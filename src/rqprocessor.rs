// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{/*Decoder,*/ Encoder, EncodingPacket, ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};
use base64;
use std::cmp;

#[derive(Debug, Clone)]
pub struct RaptorQEncoder {

    symbol_size: u16,
    symbols_per_block: u8,
    redundancy_factor: u8,
    repair_symbols: u32,
    enc: Encoder,
}

impl RaptorQEncoder {

    pub fn new(symbol_size: u16, symbols_per_block: u8, redundancy_factor: u8, data: &Vec<u8>) -> Self {
        let repair_symbols = (dbg!(data.len()) as u32) *
            (u32::from(redundancy_factor)-1) /
                u32::from(symbol_size);

        RaptorQEncoder {
            symbol_size,
            symbols_per_block,
            redundancy_factor,
            repair_symbols,
            enc: Encoder::with_defaults(&data, symbol_size)
        }
    }

    pub fn get_names(&self) -> Vec<String> {

        let mut names: Vec<String> = Vec::new();
        self.encode( |packet| {
            let mut hasher = Sha3_256::new();
            for pkt in packet {
                hasher.update(pkt.serialize());
            }
            names.push(base64::encode(hasher.finalize()));
        });

        names
    }

    pub fn get_packets(&self) -> Vec<Vec<u8>> {

        let mut chunks: Vec<Vec<u8>> = Vec::new();
        self.encode( |packet| {
            let mut chunk = Vec::new();
            for pkt in packet {
                chunk.extend(pkt.serialize());
            }
            chunks.push(chunk);
        });

        chunks
    }

    fn encode<F>(&self, mut f: F)
        where F: FnMut(&[EncodingPacket]) {

        let symbols = self.enc.get_encoded_packets(self.repair_symbols);
        println!("block_encoders - {}", self.enc.get_block_encoders().len());

        let vec_size = dbg!(symbols.len());

        let mut first : usize = 0;
        let mut next = cmp::min(self.symbols_per_block as usize, vec_size);
        while next <= vec_size {

            f(&symbols[first..next]);

            if next == vec_size {
                break;
            }

            first = next;
            next = cmp::min(first + self.symbols_per_block as usize, vec_size);
        }
    }

    pub fn serialized_encoder_info(&self) -> [u8; 12] {
        self.enc.get_config().serialize()
    }

    pub fn encoder_info(&self) -> ObjectTransmissionInformation {
        self.enc.get_config()
    }
}

// pub fn decode(&self, details: EncoderDetails, data: &Vec<u8>) -> (Vec(u8)) {
//
//     let mut decoder = Decoder::new(enc_config);
//
//     // Perform the decoding
//     let mut result = None;
//     while !packets.is_empty() {
//         result = decoder.decode(EncodingPacket::deserialize(&packets.pop().unwrap()));
//         if result != None {
//             break;
//         }
//     }
// }

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

        let rq = RaptorQEncoder::new(50000, 40, 12, &vec);

        let start = Instant::now();

        let mut names = dbg!(rq.get_names().len());

        println!("{:?} spent to create symbols' names", start.elapsed());

        let tr_length = dbg!(rq.encoder_info()).transfer_length();
        assert_eq!(tr_length as usize, v_size);
        // assert_eq!(names.len(), );
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

// let enc_config = encoder.get_config();
//
// {
// }
// // The Decoder MUST be constructed with the configuration of the Encoder.
// // The ObjectTransmissionInformation configuration should be transmitted over a reliable
// // channel
// let mut decoder = Decoder::new(enc_config);
//
// // Perform the decoding
// let mut result = None;
// while !packets.is_empty() {
//     result = decoder.decode(EncodingPacket::deserialize(&packets.pop().unwrap()));
//     if result != None {
//         break;
//     }
// }
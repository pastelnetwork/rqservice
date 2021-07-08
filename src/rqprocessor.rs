// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use sha3::{Digest, Sha3_256};
use base64;
use std::cmp;

#[derive(Debug, Default, Clone)]
pub struct RaptorQProcessor {

    symbol_size: u16,
    symbols_per_block: u8,
    redundancy_factor: u8,
}

pub struct OTI {
    pub coti: u64,
    pub ssoti: u32
}

impl RaptorQProcessor {

    pub fn new(symbol_size: u16, symbols_per_block: u8, redundancy_factor: u8) -> Self {
        RaptorQProcessor {
            symbol_size,
            symbols_per_block,
            redundancy_factor
        }
    }

    pub fn get_names(&self, data: &Vec<u8>) -> (Vec<String>, OTI) {

        let mut names: Vec<String> = Vec::new();
        let oti = self.encode( &data, |packet| {
            let mut hasher = Sha3_256::new();
            for pkt in packet {
                hasher.update(pkt.serialize());
            }
            names.push(base64::encode(hasher.finalize()));
        });

        (
            names,
            oti
        )
    }

    pub fn get_packets(&self, data: &Vec<u8>) -> (Vec<Vec<u8>>, OTI) {

        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let oti = self.encode( &data, |packet| {
            let mut chunk = Vec::new();
            for pkt in packet {
                chunk.extend(pkt.serialize());
            }
            chunks.push(chunk);
        });

        (
            chunks,
            oti
        )
    }

    fn encode<F>(&self, data: &Vec<u8>, mut f: F) -> OTI
        where F: FnMut(&[EncodingPacket]) {

        let repair_symbols = self.repair_symbols(data.len());
        println!("data size = {} number of repair symbols = {}", data.len(), repair_symbols);

        let encoder = Encoder::with_defaults(&data, self.symbol_size);
        let symbols = encoder.get_encoded_packets(repair_symbols);

        let vec_size = symbols.len();
        println!("number of all symbols = {}", vec_size);

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

        RaptorQProcessor::config_2_oti(encoder.get_config())
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

    fn config_2_oti(config: raptorq::ObjectTransmissionInformation) -> OTI {

        OTI {
            coti: (config.transfer_length() << 24) |
                u64::from(config.symbol_size()),

            ssoti: (u32::from(config.source_blocks()) << 24) |
                (u32::from(config.sub_blocks()) << 8) |
                u32::from(config.symbol_alignment())
        }
    }

    fn repair_symbols(&self, data_len: usize) -> u32 {
        (data_len as u32) *
            (u32::from(self.redundancy_factor)-1) /
                u32::from(self.symbol_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepare(v_size: u32, extra: u16) {
        let mut vec : Vec<u8> = Vec::new();
        let jm = v_size/250;
        for _j in 1..=jm {
            for i in 1..=250 {
                vec.push(i);
            }
        }
        for i in 0..extra {
            vec.push(i as u8);
        }

        println!("Start encoding...");

        let rq = RaptorQProcessor::new(
            50000, 40, 12);
        let (names, oti) = rq.get_names(&vec);

        println!("Encoded!...");

        println!("coti = {}, ssoti = {}, number of packets = {}", oti.coti, oti.ssoti, names.len());
    }

    #[test]
    fn encode_data_10000() {
        prepare(10000, 0);
    }

    #[test]
    fn encode_data_10000000() {
        prepare(10000000, 0);
    }

    #[test]
    fn encode_data_10000001() {
        prepare(10000000, 1);
    }
}

// fn create_symbol_file(pkt: &Vec<u8>) -> std::io::Result<()> {
//     let mut s = DefaultHasher::new();
//     pkt.hash(&mut s);
//     let h = s.finish();
//     let mut path = String::from("/home/alexey/work/Pastel/rq-service/out/");
//     base64_url::encode_to_string(&format!("{:x}", h), &mut path);
//     let mut file = File::create(path)?;
//     file.write_all(&pkt)
// }

// let mut data: Vec<u8> = Vec::new();
// let mut input = File::open("/home/alexey/work/Pastel/rq-service/raptor.jpg")?;
// input.read_to_end(&mut data)?;
//
// // Create the Encoder, with an MTU of 1400 (common for Ethernet)
// let encoder = Encoder::with_defaults(&data, 65535);
//
// let packets = encoder.get_encoded_packets(15);
// for packet in &packets {
//     let pkt = packet.serialize();
//     create_symbol_file(&pkt)?
// };
//

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
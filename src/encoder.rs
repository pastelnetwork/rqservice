// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

// use raptorq::{Decoder, Encoder, EncodingPacket};
// use std::fs::File;
// use std::io::Read;
// use std::io::Write;
// use std::collections::hash_map::DefaultHasher;
// use std::hash::{Hash, Hasher};
// use base64_url;

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
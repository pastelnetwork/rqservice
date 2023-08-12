// Copyright (c) 2021-2023 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use crate::app::ServiceSettings;

use tonic::{transport::Server, Request, Response, Status};
use std::path::Path;

pub mod rq {
    tonic::include_proto!("raptorq");
}
use rq::raptor_q_server::{RaptorQ, RaptorQServer};
use rq::{EncodeMetaDataRequest, EncodeMetaDataReply, EncodeRequest, EncodeReply, DecodeRequest, DecodeReply};

use crate::rqprocessor;
use crate::rqprocessor::DB_PATH;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

// #[derive(Debug, Default)]
pub struct RaptorQService {
    pub settings: ServiceSettings,
    pub pool: Pool<SqliteConnectionManager>, // Added pool field
}

impl Default for RaptorQService {
    fn default() -> Self {
        let manager = SqliteConnectionManager::file(DB_PATH);
        let pool = r2d2::Pool::builder()
            .build(manager)
            .expect("Failed to create pool");

        RaptorQService {
            settings: ServiceSettings::default(), // Assuming you have a default for ServiceSettings
            pool, // Use the created pool
        }
    }
}


#[tonic::async_trait]

impl RaptorQ for RaptorQService {

    async fn encode_meta_data(&self, request: Request<EncodeMetaDataRequest>) -> Result<Response<EncodeMetaDataReply>, Status> {
        log::info!("Got an 'encoder_info' request: {:?}", request);
    
        let processor = rqprocessor::RaptorQProcessor::new(
            self.settings.symbol_size,
            self.settings.redundancy_factor);
    
        let req = request.into_inner();
    
        // Use the entire connection pool, not just a single connection
        let pool = &self.pool; // Make sure to access the pool in your specific context
    
        match processor.create_metadata_and_store(&req.path, &req.block_hash, &req.pastel_id, pool) {
            Ok((meta, path)) => {
                // Build the reply using the meta and path
                let reply = rq::EncodeMetaDataReply {
                    encoder_parameters: meta.encoder_parameters,
                    symbols_count: meta.source_symbols + meta.repair_symbols,
                    path,
                };
    
                Ok(Response::new(reply))
            },
            Err(e) => {
                // Handle error case
                log::error!("Error while processing metadata: {:?}", e);
                Err(Status::internal("Error while processing metadata"))
            }
        }
    }
    

    async fn encode(&self, request: Request<EncodeRequest>) -> Result<Response<EncodeReply>, Status> {
        log::info!("Got a 'encode' request: {:?}", request);
    
        let processor = rqprocessor::RaptorQProcessor::new(
            self.settings.symbol_size,
            self.settings.redundancy_factor);
    
        let req = request.into_inner();
        
        // Use the connection pool that was created in main.rs
        let pool = &self.pool; // Adjust this line to access the pool in your specific context
        
        match processor.encode(&req.path, pool) { // Pass the pool as the second argument
            Ok((meta, path)) => {
                let reply = rq::EncodeReply {
                    encoder_parameters: meta.encoder_parameters,
                    symbols_count: meta.source_symbols + meta.repair_symbols,
                    path };
    
                Ok(Response::new(reply))
            },
            Err(e) => {
                log::error!("Internal error: {:?}", e);
                Err(Status::internal("Internal error"))
            }
        }
    }
    

    async fn decode(&self, request: Request<DecodeRequest>) -> Result<Response<DecodeReply>, Status> {
        log::info!("Got a 'decode' request: {:?}", request);
    
        let processor = rqprocessor::RaptorQProcessor::new(
            self.settings.symbol_size,
            self.settings.redundancy_factor);
    
        // Obtain a database connection
        let conn = match self.pool.get() {
            Ok(connection) => connection,
            Err(e) => {
                log::error!("Database connection error: {:?}", e);
                return Err(Status::internal("Database connection error"));
            }
        };
        
    
        let req = request.into_inner();
    
        // Convert the provided path to a Path object
        let input_path = Path::new(&req.path);
    
        // Compute the original file hash
        let original_file_hash = match processor.compute_original_file_hash(&input_path) {
            Ok(hash) => hash,
            Err(e) => {
                log::error!("File hash computation error: {:?}", e);
                return Err(Status::internal("File hash computation error"));
            }
        };
    
        // Check the length of encoder_parameters and ensure it is as expected
        if req.encoder_parameters.len() != 12 {
            return Err(Status::invalid_argument("Invalid encoder_parameters length"));
        }
    
        match processor.decode(&conn, &req.encoder_parameters, &original_file_hash) {
            Ok(path) => {
                let reply = rq::DecodeReply { path };
                Ok(Response::new(reply))
            },
            Err(e) => {
                log::error!("Internal error: {:?}", e);
                Err(Status::internal("Internal error"))
            }
        }
    }
}


pub async fn start_server(settings: &ServiceSettings, pool: &Pool<SqliteConnectionManager>) -> Result<(), Box<dyn std::error::Error>> {
    let addr = settings.grpc_service.parse().unwrap();

    log::info!("RaptorQ gRPC Server listening on {}", addr);

    let raptorq_service = RaptorQService {
        settings: settings.clone(),
        pool: pool.clone(), // Added pool to the RaptorQService instance
    };
    let srv = RaptorQServer::new(raptorq_service);

    Server::builder().add_service(srv).serve(addr).await?;

    Ok(())
}

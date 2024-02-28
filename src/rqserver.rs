// Copyright (c) 2021-2023 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use crate::app::ServiceSettings;

use tonic::{transport::Server, Request, Response, Status};

#[allow(non_snake_case)]
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
        let manager = SqliteConnectionManager::file(&**rqprocessor::DB_PATH);
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
        log::info!("Received 'encode_meta_data' request: {:?}", request);
    
        log::debug!("Creating RaptorQProcessor...");
        let processor = match rqprocessor::RaptorQProcessor::new(DB_PATH) {
            Ok(processor) => processor,
            Err(err) => {
                log::error!("Failed to create processor: {:?}", err);
                return Err(Status::internal("Failed to create processor"));
            }
        };
    
        let req = request.into_inner();
    
        // Get a DB connection from the pool for the `fetch_metadata_from_db` call
        let conn = match self.pool.get() {
            Ok(conn) => conn,
            Err(err) => {
                log::error!("Failed to get DB connection: {:?}", err);
                return Err(Status::internal("Failed to get DB connection"));
            }
        };
    
        // Compute the hash of the file at the given path
        log::debug!("Computing hash for the file at path: {}", &req.path);
        let file_path = std::path::Path::new(&req.path);
        let original_file_hash = match processor.compute_original_file_hash(file_path) {
            Ok(hash) => hash,
            Err(e) => {
                log::error!("Failed to compute hash for file at path {}: {:?}", &req.path, e);
                return Err(Status::internal("Failed to compute file hash"));
            }
        };
    
        // Fetch existing metadata using the computed hash
        log::debug!("Attempting to fetch existing metadata...");
        match processor.fetch_metadata_from_db(&conn, &original_file_hash) {
            Ok((original_file_path, _original_file_size_in_mb, files_number, encoder_parameters, _block_hash, _pastel_id, _symbol_ids)) => {
                log::debug!("Found existing metadata, encoding...");
                let reply = rq::EncodeMetaDataReply {
                    encoder_parameters: encoder_parameters,
                    symbols_count: files_number, // Assuming 'files_number' represents total symbols
                    path: original_file_path,
                };
                Ok(Response::new(reply))
            },
            Err(_e) => {
                log::debug!("No existing metadata found or error occurred, attempting to create new metadata...");
                // Correctly passing all required arguments to `create_metadata`
                match processor.create_metadata(&req.path, &req.block_hash, &req.pastel_id, &self.pool) {
                    Ok((meta, path)) => {
                        log::debug!("Successfully processed new metadata.");
                        let reply = rq::EncodeMetaDataReply {
                            encoder_parameters: meta.encoder_parameters,
                            symbols_count: meta.source_symbols + meta.repair_symbols,
                            path,
                        };
                        Ok(Response::new(reply))
                    },
                    Err(e) => {
                        log::error!("Error while processing metadata: {:?}", e);
                        let error_message = format!("Error while processing metadata: {}", e.to_string());
                        Err(Status::internal(error_message))
                    }
                }
            }
        }
    }
    

    async fn encode(&self, request: Request<EncodeRequest>) -> Result<Response<EncodeReply>, Status> {
        log::info!("Received 'encode' request: {:?}", request);

        // Create the RaptorQProcessor with the specified DB path
        log::debug!("Creating RaptorQProcessor...");
        let processor_result = rqprocessor::RaptorQProcessor::new(DB_PATH);

        let processor = match processor_result {
            Ok(processor) => processor,
            Err(err) => {
                log::error!("Failed to create processor: {:?}", err);
                return Err(Status::internal("Failed to create processor"));
            }
        };
        let req = request.into_inner();
        
        // Use the connection pool that was created in main.rs
        let pool = &self.pool; // Adjust this line to access the pool in your specific context
        
        log::debug!("Calling 'encode' method...");
        match processor.encode(&req.path, pool) { // Pass the pool as the second argument
            Ok((meta, path)) => {
                log::debug!("Successfully encoded.");
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
        log::info!("Received 'decode' request: {:?}", request);
    
        // Create the RaptorQProcessor with the specified DB path
        let processor = rqprocessor::RaptorQProcessor::new(DB_PATH)
            .map_err(|err| {
                log::error!("Failed to create processor: {:?}", err);
                Status::internal("Failed to create processor")
            })?;
    
        let req: DecodeRequest = request.into_inner();
    
        // Check the length of encoder_parameters and ensure it is as expected
        if req.encoder_parameters.len() != 12 {
            return Err(Status::invalid_argument("Invalid encoder_parameters length"));
        }
    
        // Convert the provided encoder_parameters to a [u8; 12] array
        let encoder_parameters_array: Vec<u8> = req.encoder_parameters
            .try_into()
            .map_err(|_| Status::internal("Failed to convert encoder_parameters to array"))?;
    
        log::info!("Calling 'decode' method...");
    
        // Pass the path to the location where the RQ symbol files are stored
        match processor.decode(&self.pool, &encoder_parameters_array, &req.path) {
            Ok(path) => {
                log::info!("Successfully decoded.");
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
    log::info!("Starting RaptorQ gRPC Server on {}", settings.grpc_service);
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

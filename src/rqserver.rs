// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use crate::app::ServiceSettings;

use tonic::{transport::Server, Request, Response, Status};

pub mod rq {
    tonic::include_proto!("raptorq");
}
use rq::raptor_q_server::{RaptorQ, RaptorQServer};
use rq::{EncodeMetaDataRequest, EncodeMetaDataReply, EncodeRequest, EncodeReply, DecodeRequest, DecodeReply};

use crate::rqprocessor;

#[derive(Debug, Default)]
pub struct RaptorQService {
    pub settings: ServiceSettings,
}

#[tonic::async_trait]
impl RaptorQ for RaptorQService {
    async fn encode_meta_data(&self, request: Request<EncodeMetaDataRequest>) -> Result<Response<EncodeMetaDataReply>, Status> {
        log::info!("Got a 'encoder_info' request: {:?}", request);

        let processor = rqprocessor::RaptorQProcessor::new(
            self.settings.symbol_size,
            self.settings.redundancy_factor);

        let req = request.into_inner();
        match processor.create_metadata(&req.path, req.files_number,
                                        &req.block_hash, &req.pastel_id) {
            Ok((meta, path)) => {

                let reply = rq::EncodeMetaDataReply {
                    encoder_parameters: meta.encoder_parameters,
                    symbols_count: meta.source_symbols+meta.repair_symbols,
                    path };

                Ok(Response::new(reply))
            },
            Err(e) => {
                log::error!("Internal error: {:?}", e);
                Err(Status::internal("Internal error"))
            }
        }
    }

    async fn encode(&self, request: Request<EncodeRequest>) -> Result<Response<EncodeReply>, Status> {
        log::info!("Got a 'encode' request: {:?}", request);

        let processor = rqprocessor::RaptorQProcessor::new(
            self.settings.symbol_size,
            self.settings.redundancy_factor);

        let req = request.into_inner();
        match processor.encode(&req.path) {
            Ok((meta, path)) => {

                let reply = rq::EncodeReply {
                    encoder_parameters: meta.encoder_parameters,
                    symbols_count: meta.source_symbols+meta.repair_symbols,
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

        let req = request.into_inner();
        match processor.decode(&req.encoder_parameters, &req.path) {
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

pub async fn start_server(settings: &ServiceSettings) -> Result<(), Box<dyn std::error::Error>> {

    let addr = settings.grpc_service.parse().unwrap();

    log::info!("RaptorQ gRPC Server listening on {}", addr);

    let raptorq_service = RaptorQService{settings: settings.clone()};
    let srv = RaptorQServer::new(raptorq_service);

    Server::builder().add_service(srv).serve(addr).await?;

    Ok(())
}
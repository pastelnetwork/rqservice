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
        match processor.create_metadata(req.path, req.files_number, req.block_hash, req.pastel_id) {
            Ok(meta) => {

                let reply = rq::EncodeMetaDataReply {
                    encoder_parameters: meta.encoder_parameters,
                    path: meta.path,
                    symbol_names: meta.symbol_names };

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

        // // creating a queue or channel
        // let (tx, rx) = mpsc::channel(10); //buffer is 10 messages
        //
        // let req = request.into_inner();
        //
        // let rq_encoder = rqprocessor::RaptorQEncoder::new(
        //     self.settings.symbol_size,
        //     self.settings.redundancy_factor,
        //     &req.data);
        // let symbols = rq_encoder.get_packets();


        let reply = rq::EncodeReply { path: String::new(), symbols_count: 0 };

        Ok(Response::new(reply))
    }
    async fn decode(&self, request: Request<DecodeRequest>) -> Result<Response<DecodeReply>, Status> {
        log::info!("Got a 'decode' request: {:?}", request);

        // let mut stream = request.into_inner();
        //
        // let mut rq_decoder: Option<rqprocessor::RaptorQDecoder> = None;

        let reply = rq::DecodeReply { path: String::new() };

        Ok(Response::new(reply))
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
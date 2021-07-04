// Copyright (c) 2021-2021 The Pastel Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

use crate::app::ServiceSettings;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

pub mod rq {
    tonic::include_proto!("raptorq");
}
use rq::raptor_q_server::{RaptorQ, RaptorQServer};
use rq::{UploadDataRequest, EncoderInfoReply, EncoderParameters, SymbolReply, UploadSymbolsRequest, DownloadDataReply};
use tokio_stream::StreamExt;

use crate::encoder;
use crate::encoder::encode;

#[derive(Debug, Default)]
pub struct RaptorQService;

#[tonic::async_trait]
impl RaptorQ for RaptorQService {
    async fn encoder_info(&self, request: Request<UploadDataRequest>) -> Result<Response<EncoderInfoReply>, Status> {
        log::info!("Got a 'encoder_info' request: {:?}", request);

        let req = request.into_inner();

        let rq_reply = encoder::encode(&req.data);

        let names : Vec<String> = rq_reply.symbols.into_iter().map(
            |symbol| encoder::symbol_id(symbol)
        ).collect();

        let encoder_params = rq::EncoderParameters {
            coti: rq_reply.coti,
            ssoti: rq_reply.ssoti
        };

        let reply = rq::EncoderInfoReply { name: names, encoder_params: Some(encoder_params) };

        Ok(Response::new(reply))
    }

    type EncodeStream=ReceiverStream<Result<SymbolReply, Status>>;

    async fn encode(&self, request: Request<UploadDataRequest>) -> Result<Response<Self::EncodeStream>, Status> {
        log::info!("Got a 'encode' request: {:?}", request);

        // creating a queue or channel
        let (tx, rx) = mpsc::channel(10); //buffer is 10 messages

        let req = request.into_inner();

        // creating a new task
        tokio::spawn(async move {
            // looping and sending our response using stream
            let rq_reply = encoder::encode(&req.data);

            for symbol in rq_reply.symbols {
                // sending response to our channel
                if let Err(e) = tx.send(Ok(SymbolReply {symbol: symbol.serialize(),})).await {
                    log::error!("Error streaming symbol {}", e)
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
    async fn decode(&self, request: Request<tonic::Streaming<UploadSymbolsRequest>>) -> Result<Response<DownloadDataReply>, Status> {
        log::info!("Got a 'decode' request: {:?}", request);

        let mut stream = request.into_inner();

        while let Some(msg) = stream.next().await {
            log::info!("Message: {:?}", msg);
            if let Ok(m) = msg {
                match m.params_or_symbols_oneof {
                    Some(rq::upload_symbols_request::ParamsOrSymbolsOneof::EncoderParams(e)) => {
                        log::info!("Get Encoder Parameters")
                    },
                    Some(rq::upload_symbols_request::ParamsOrSymbolsOneof::Symbol(s)) => {
                        log::info!("Get Symbol")
                    },
                    None => ()
                }
            }
        }

        Ok(Response::new(rq::DownloadDataReply { data: Vec::new()}))
    }
}

pub async fn start_server(settings: &ServiceSettings) -> Result<(), Box<dyn std::error::Error>> {

    let addr = settings.grpc_service.parse().unwrap();

    log::info!("RemoteCliServer listening on {}", addr);

    let raptorq_service = RaptorQService::default();
    let srv = RaptorQServer::new(raptorq_service);

    Server::builder().add_service(srv).serve(addr).await?;

    Ok(())
}
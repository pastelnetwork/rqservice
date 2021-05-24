use crate::app::ServiceSettings;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

pub mod rqservice_proto {
    tonic::include_proto!("rqservice");
}
use rqservice_proto::rq_service_server::{RqService, RqServiceServer};
use rqservice_proto::{UploadDataRequest, EncoderInfoReply, EncoderParameters, SymbolReply, UploadSymbolsRequest, DownloadDataReply};

#[derive(Debug, Default)]
pub struct RaptorQService;

#[tonic::async_trait]
impl RqService for RaptorQService {
    async fn encoder_info(&self, request: Request<UploadDataRequest>) -> Result<Response<EncoderInfoReply>, Status> {
        println!("Got a request: {:?}", request);

        // let req = request.into_inner();
        // let a = req.data;

        let names = vec![String::from("1"), String::from("2"), String::from("3"), String::from("4"), String::from("5")];
        let encoder_params = rqservice_proto::EncoderParameters {
            transfer_length: 100,
            symbol_size: 10,
            num_source_blocks: 1,
            num_sub_blocks: 1,
            symbol_alignment: 0,
        };

        let reply = rqservice_proto::EncoderInfoReply { name: names, encoder_params: Some(encoder_params) };

        Ok(Response::new(reply))
    }

    type EncodeStream=ReceiverStream<Result<SymbolReply,Status>>;

    async fn encode(&self, _request: Request<UploadDataRequest>) -> Result<Response<Self::EncodeStream>, Status> {

        // creating a queue or channel
        let (tx, rx) = mpsc::channel(4);

        // creating a new task
        tokio::spawn(async move {
            // looping and sending our response using stream
            for i in 0..4 {
                // sending response to our channel
                let v = vec![i];
                tx.send(Ok(SymbolReply {symbol: v,})).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
    async fn decode(&self, _request: Request<UploadSymbolsRequest>) -> Result<Response<DownloadDataReply>, Status> {
        Ok(Response::new(rqservice_proto::DownloadDataReply { data: Vec::new()}))
    }
}

pub async fn start_server(settings: ServiceSettings) -> Result<(), Box<dyn std::error::Error>> {

    let addr = settings.grpc_service.parse().unwrap();

    println!("RemoteCliServer listening on {}", addr);

    let rd_ervice = RaptorQService::default();
    let srv = RqServiceServer::new(rd_ervice);

    Server::builder().add_service(srv).serve(addr).await?;

    Ok(())
}
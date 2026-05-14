use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::{Stream, TryStreamExt};
use loom_engine::{parse_query_json, Engine};
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

pub(crate) struct LoomFlightService {
    pub(crate) engine: Arc<Engine>,
}

type BoxFlightStream<T> = BoxStream<'static, Result<T, Status>>;

#[tonic::async_trait]
impl FlightService for LoomFlightService {
    type HandshakeStream = BoxFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxFlightStream<FlightInfo>;
    type DoGetStream = BoxFlightStream<FlightData>;
    type DoPutStream = BoxFlightStream<PutResult>;
    type DoExchangeStream = BoxFlightStream<FlightData>;
    type DoActionStream = BoxFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxFlightStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let s = futures::stream::once(async {
            Ok(HandshakeResponse {
                protocol_version: 0,
                payload: Vec::new().into(),
            })
        });
        Ok(Response::new(Box::pin(s)))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let json = std::str::from_utf8(ticket.ticket.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let query = parse_query_json(json).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let batches = self
            .engine
            .query_batches(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let stream = flight_data_stream(batches)?;
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }
}

fn flight_data_stream(
    batches: Vec<RecordBatch>,
) -> Result<impl Stream<Item = Result<FlightData, Status>>, Status> {
    let input = futures::stream::iter(
        batches
            .into_iter()
            .map(Ok::<RecordBatch, arrow_flight::error::FlightError>),
    );
    let stream = FlightDataEncoderBuilder::new()
        .build(input)
        .map_err(Status::from);
    Ok(stream)
}

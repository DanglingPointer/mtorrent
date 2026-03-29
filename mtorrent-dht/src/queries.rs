mod handler;
mod incoming;

#[cfg(test)]
mod tests;

use super::error::Error;
use super::msgs::*;
use super::udp;
use futures_util::StreamExt;
use handler::Handler;
use local_async_utils::prelude::*;
use mtorrent_utils::{debug_stopwatch, trace_stopwatch};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::rc::Rc;
use tokio::select;
use tokio::sync::{Semaphore, mpsc};

pub(super) use incoming::*;

struct OutgoingQuery {
    query: QueryMsg,
    destination_addr: SocketAddr,
    response_sink: local_oneshot::Sender<Result<ResponseMsg, Error>>,
}

/// Client for sending outgoing queries to different nodes.
#[derive(Clone)]
pub struct OutboundQueries {
    channel: local_unbounded::Sender<OutgoingQuery>,
    query_slots: Rc<Semaphore>,
}

impl OutboundQueries {
    pub(super) async fn ping(
        &self,
        destination: SocketAddr,
        query: PingArgs,
    ) -> Result<PingResponse, Error> {
        let _sw = trace_stopwatch!("Ping query to {destination}");
        self.do_query(destination, query).await
    }

    pub(super) async fn find_node(
        &self,
        destination: SocketAddr,
        query: FindNodeArgs,
    ) -> Result<FindNodeResponse, Error> {
        let _sw = trace_stopwatch!("FindNode query to {destination}");
        self.do_query(destination, query).await
    }

    pub(super) async fn get_peers(
        &self,
        destination: SocketAddr,
        query: GetPeersArgs,
    ) -> Result<GetPeersResponse, Error> {
        let _sw = trace_stopwatch!("GetPeers query to {destination}");
        self.do_query(destination, query).await
    }

    pub(super) async fn announce_peer(
        &self,
        destination: SocketAddr,
        query: AnnouncePeerArgs,
    ) -> Result<AnnouncePeerResponse, Error> {
        let _sw = trace_stopwatch!("AnnouncePeer query to {destination}");
        self.do_query(destination, query).await
    }

    async fn do_query<Q, R>(&self, dst_addr: SocketAddr, args: Q) -> Result<R, Error>
    where
        Q: Into<QueryMsg> + Debug,
        R: TryFrom<ResponseMsg, Error = Error> + Debug,
    {
        let _slot = self.query_slots.acquire().await;
        let (tx, rx) = local_oneshot::channel();
        log::trace!("[{dst_addr}] <= {args:?}");
        self.channel.send(OutgoingQuery {
            query: args.into(),
            destination_addr: dst_addr,
            response_sink: tx,
        })?;
        let result = rx.await.ok_or(Error::ChannelClosed)?.and_then(R::try_from);
        match &result {
            Ok(response) => log::trace!("[{dst_addr}] => {response:?}"),
            Err(Error::ErrorResponse(msg)) => log::debug!("[{dst_addr}] => {msg:?}"),
            Err(Error::Timeout) => log::trace!("Query to {dst_addr} timed out"),
            Err(e) => log::debug!("Query to {dst_addr} failed: {e}"),
        }
        result
    }
}

/// Server for receiving incoming queries from different nodes.
pub struct InboundQueries(pub(super) local_unbounded::Receiver<IncomingQuery>);

/// Actor that routes queries between [`Processor`](crate::Processor) and
/// [`IoDriver`](crate::IoDriver), performs retries and matches requests and responses.
pub struct QueryRouter {
    handler: Handler,
    outgoing_queries_source: local_unbounded::Receiver<OutgoingQuery>,
    incoming_msgs_source: mpsc::Receiver<(Message, SocketAddr)>,
}

impl QueryRouter {
    pub async fn run(mut self) {
        let _sw = debug_stopwatch!("Queries runner");
        loop {
            select! {
                biased;
                outgoing = self.outgoing_queries_source.next() => {
                    let Some(query) = outgoing else { break };
                    if let Err(e) = self.handler.handle_outgoing(query).await {
                        log::warn!("Error while handling outbound query: {e}");
                        break;
                    }
                }
                incoming = self.incoming_msgs_source.recv() => {
                    let Some(msg) = incoming else { break };
                    if let Err(e) = self.handler.handle_incoming(msg).await {
                        log::warn!("Error while handling inbound query: {e}");
                        break;
                    }
                }
                true = self.handler.handle_next_timeout() => {}
            }
        }
    }
}

/// Create the layer that facilitates inbound and outbound transactions (queries).
pub fn setup_queries(
    udp::MessageChannelSender(outgoing_msgs_sink): udp::MessageChannelSender,
    udp::MessageChannelReceiver(incoming_msgs_source): udp::MessageChannelReceiver,
    max_concurrent_queries: Option<usize>,
) -> (OutboundQueries, InboundQueries, QueryRouter) {
    let (outgoing_queries_sink, outgoing_queries_source) = local_unbounded::channel();
    let (incoming_queries_sink, incoming_queries_source) = local_unbounded::channel();
    let max_in_flight = max_concurrent_queries.unwrap_or(udp::MSG_QUEUE_LEN);

    let runner = QueryRouter {
        handler: Handler::new(outgoing_msgs_sink, incoming_queries_sink),
        outgoing_queries_source,
        incoming_msgs_source,
    };
    let client = OutboundQueries {
        channel: outgoing_queries_sink,
        query_slots: Rc::new(Semaphore::const_new(max_in_flight)),
    };
    if max_in_flight == 0 {
        client.query_slots.close();
    }
    (client, InboundQueries(incoming_queries_source), runner)
}

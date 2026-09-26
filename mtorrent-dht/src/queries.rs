mod client;
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
use mtorrent_utils::debug_stopwatch;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;

pub(super) use client::*;
pub(super) use incoming::*;

/// Create the layer that facilitates inbound and outbound transactions (queries).
pub fn setup_queries(
    udp::MessageChannelSender(outgoing_msgs_sink): udp::MessageChannelSender,
    udp::MessageChannelReceiver(incoming_msgs_source): udp::MessageChannelReceiver,
    query_timeout: Option<Duration>,
) -> (OutboundQueries, InboundQueries, QueryRouter) {
    let (outgoing_queries_sink, outgoing_queries_source) = local_unbounded::channel();
    let (incoming_queries_sink, incoming_queries_source) = local_unbounded::channel();

    let runner = QueryRouter {
        handler: Handler::new(outgoing_msgs_sink, incoming_queries_sink, query_timeout),
        outgoing_queries_source,
        incoming_msgs_source,
    };
    (OutboundQueries(outgoing_queries_sink), InboundQueries(incoming_queries_source), runner)
}

/// Sink for outbound queries to different nodes.
#[derive(Clone)]
pub struct OutboundQueries(local_unbounded::Sender<OutgoingQuery>);

/// Source of incoming queries from different nodes.
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

struct OutgoingQuery {
    query: QueryMsg,
    destination_addr: SocketAddr,
    response_sink: local_oneshot::Sender<Result<ResponseMsg, Error>>,
}

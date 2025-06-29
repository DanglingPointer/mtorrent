mod manager;

#[cfg(test)]
mod tests;

use super::error::Error;
use super::{U160, msgs::*, udp};
use crate::{debug_stopwatch, trace_stopwatch};
use derive_more::derive::From;
use futures::StreamExt;
use local_async_utils::prelude::*;
use manager::{OutgoingQuery, QueryManager};
use std::fmt::Debug;
use std::future::pending;
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;
use std::rc::Rc;
use tokio::select;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::{Instant, sleep_until};

/// Client for sending outgoing queries to different nodes.
#[derive(Clone)]
pub struct Client {
    channel: local_channel::Sender<OutgoingQuery>,
    query_slots: Rc<Semaphore>,
}

impl Client {
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
        let (tx, rx) = local_oneshot::oneshot();
        log::trace!("[{dst_addr}] <= {args:?}");
        self.channel.send(OutgoingQuery {
            query: args.into(),
            destination_addr: dst_addr,
            response_sink: tx,
        });
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
pub struct Server(pub(super) local_channel::Receiver<IncomingQuery>);

/// Actor that routes queries between app layer and network layer.
pub struct Runner {
    queries: QueryManager,
    outgoing_queries_source: local_channel::Receiver<OutgoingQuery>,
    incoming_msgs_source: mpsc::Receiver<(Message, SocketAddr)>,
}

impl Runner {
    pub async fn run(mut self) -> Result<(), Error> {
        let _sw = debug_stopwatch!("Queries runner");
        loop {
            let next_timeout = self.queries.next_timeout();
            select! {
                biased;
                outgoing = self.outgoing_queries_source.next() => {
                    match outgoing {
                        None => break,
                        Some(query) => self.queries.handle_one_outgoing(query).await?,
                    }
                }
                incoming = self.incoming_msgs_source.recv() => {
                    match incoming {
                        None => break,
                        Some(msg) => self.queries.handle_one_incoming(msg).await?,
                    }
                }
                _ = Self::sleep_until(next_timeout), if next_timeout.is_some() => {
                    self.queries.handle_timeouts().await?;
                }
            }
        }
        Ok(())
    }

    async fn sleep_until(deadline: Option<Instant>) {
        #[cfg(not(test))]
        match deadline {
            Some(deadline) => sleep_until(deadline).await,
            _ => pending::<()>().await,
        }

        #[cfg(test)]
        match deadline {
            Some(deadline) if tests::SLEEP_ENABLED.get() => sleep_until(deadline).await,
            _ => pending::<()>().await,
        }
    }
}

// ------------------------------------------------------------------------------------------------

pub fn setup_routing(
    outgoing_msgs_sink: mpsc::Sender<(Message, SocketAddr)>,
    incoming_msgs_source: mpsc::Receiver<(Message, SocketAddr)>,
    max_concurrent_queries: Option<usize>,
) -> (Client, Server, Runner) {
    let (outgoing_queries_sink, outgoing_queries_source) = local_channel::channel();
    let (incoming_queries_sink, incoming_queries_source) = local_channel::channel();
    let max_in_flight = max_concurrent_queries.unwrap_or(udp::MSG_QUEUE_LEN);

    let runner = Runner {
        queries: QueryManager::new(outgoing_msgs_sink, incoming_queries_sink),
        outgoing_queries_source,
        incoming_msgs_source,
    };
    let client = Client {
        channel: outgoing_queries_sink,
        query_slots: Rc::new(Semaphore::const_new(max_in_flight)),
    };
    if max_in_flight == 0 {
        client.query_slots.close();
    }
    (client, Server(incoming_queries_source), runner)
}

// ------------------------------------------------------------------------------------------------

#[cfg_attr(test, derive(Debug))]
#[derive(From)]
pub(super) enum IncomingQuery {
    Ping(IncomingPingQuery),
    FindNode(IncomingFindNodeQuery),
    GetPeers(IncomingGetPeersQuery),
    AnnouncePeer(IncomingAnnouncePeerQuery),
}

#[cfg_attr(test, derive(Debug))]
pub(super) struct IncomingGenericQuery<Q, R> {
    transaction_id: Vec<u8>,
    query: Q,
    response_sink: Option<mpsc::OwnedPermit<(Message, SocketAddr)>>,
    source_addr: SocketAddr,
    _stopwatch: Stopwatch,
    _response_type: PhantomData<R>,
}

pub(super) type IncomingPingQuery = IncomingGenericQuery<PingArgs, PingResponse>;
pub(super) type IncomingFindNodeQuery = IncomingGenericQuery<FindNodeArgs, FindNodeResponse>;
pub(super) type IncomingGetPeersQuery = IncomingGenericQuery<GetPeersArgs, GetPeersResponse>;
pub(super) type IncomingAnnouncePeerQuery =
    IncomingGenericQuery<AnnouncePeerArgs, AnnouncePeerResponse>;

impl IncomingQuery {
    fn new(
        incoming: QueryMsg,
        tid: Vec<u8>,
        sink: mpsc::OwnedPermit<(Message, SocketAddr)>,
        remote_addr: SocketAddr,
    ) -> IncomingQuery {
        macro_rules! convert {
            ($query_args:expr, $name:literal) => {{
                log::trace!("[{}] => {:?}", remote_addr, $query_args);
                IncomingQuery::from(IncomingGenericQuery {
                    transaction_id: tid,
                    query: $query_args,
                    response_sink: Some(sink),
                    source_addr: remote_addr,
                    _stopwatch: trace_stopwatch!("{} query from {}", $name, remote_addr),
                    _response_type: PhantomData,
                })
            }};
        }
        match incoming {
            QueryMsg::Ping(args) => convert!(args, "Ping"),
            QueryMsg::FindNode(args) => convert!(args, "FindNode"),
            QueryMsg::GetPeers(args) => convert!(args, "GetPeers"),
            QueryMsg::AnnouncePeer(args) => convert!(args, "AnnouncePeer"),
        }
    }

    #[expect(dead_code)]
    pub(super) fn node_id(&self) -> &U160 {
        match self {
            IncomingQuery::Ping(q) => &q.args().id,
            IncomingQuery::FindNode(q) => &q.args().id,
            IncomingQuery::GetPeers(q) => &q.args().id,
            IncomingQuery::AnnouncePeer(q) => &q.args().id,
        }
    }

    pub(super) fn source_addr(&self) -> &SocketAddr {
        match self {
            IncomingQuery::Ping(q) => q.source_addr(),
            IncomingQuery::FindNode(q) => q.source_addr(),
            IncomingQuery::GetPeers(q) => q.source_addr(),
            IncomingQuery::AnnouncePeer(q) => q.source_addr(),
        }
    }
}

impl<Q, R> IncomingGenericQuery<Q, R> {
    pub(super) fn args(&self) -> &Q {
        &self.query
    }

    pub(super) fn source_addr(&self) -> &SocketAddr {
        &self.source_addr
    }

    pub(super) fn respond(mut self, response: R) -> Result<(), Error>
    where
        R: Into<ResponseMsg> + Debug,
    {
        log::trace!("[{}] <= {:?}", self.source_addr, response);
        let sender = self.response_sink.take().unwrap_or_else(|| unreachable!()).send((
            Message {
                transaction_id: mem::take(&mut self.transaction_id),
                version: None,
                data: MessageData::Response(response.into()),
            },
            self.source_addr,
        ));
        if sender.is_closed() {
            Err(Error::ChannelClosed)
        } else {
            Ok(())
        }
    }

    pub(super) fn respond_error(mut self, error: ErrorMsg) -> Result<(), Error> {
        log::debug!("[{}] <= {:?}", self.source_addr, error);
        let sender = self.response_sink.take().unwrap_or_else(|| unreachable!()).send((
            Message {
                transaction_id: mem::take(&mut self.transaction_id),
                version: None,
                data: MessageData::Error(error),
            },
            self.source_addr,
        ));
        if sender.is_closed() {
            Err(Error::ChannelClosed)
        } else {
            Ok(())
        }
    }
}

impl<Q, R> Drop for IncomingGenericQuery<Q, R> {
    fn drop(&mut self) {
        if let Some(sink) = self.response_sink.take() {
            let error_msg = ErrorMsg {
                error_code: ErrorCode::Server,
                error_msg: "Unable to handle query".to_string(),
            };
            log::warn!("[{}] <= {:?}", self.source_addr, error_msg);
            sink.send((
                Message {
                    transaction_id: mem::take(&mut self.transaction_id),
                    version: None,
                    data: MessageData::Error(error_msg),
                },
                self.source_addr,
            ));
        }
    }
}

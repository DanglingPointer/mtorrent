mod manager;

#[cfg(test)]
mod tests;

use super::error::Error;
use super::{msgs::*, U160};
use crate::trace_stopwatch;
use crate::utils::local_sync;
use crate::utils::stopwatch::Stopwatch;
use derive_more::derive::From;
use futures::StreamExt;
use manager::{OutgoingQuery, QueryManager};
use std::fmt::Debug;
use std::future::pending;
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::{sleep_until, Instant};

/// Client for sending outgoing queries to different nodes.
#[derive(Clone)]
pub struct Client(local_sync::channel::Sender<OutgoingQuery>);

#[allow(dead_code)]
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
        let (tx, rx) = local_sync::oneshot();
        log::debug!("[{dst_addr}] <= {args:?}");
        self.0.send(OutgoingQuery {
            query: args.into(),
            destination_addr: dst_addr,
            response_sink: tx,
        });
        let result = rx.await.ok_or(Error::ChannelClosed)?.and_then(R::try_from);
        match &result {
            Ok(response) => log::debug!("[{dst_addr}] => {response:?}"),
            Err(Error::ErrorResponse(msg)) => log::error!("[{dst_addr}] => {msg:?}"),
            Err(e) => log::error!("Query to {dst_addr} failed: {e:?}"),
        }
        result
    }
}

/// Server for receiving incoming queries from different nodes.
pub struct Server(pub(super) local_sync::channel::Receiver<IncomingQuery>);

/// Actor that routes queries between app layer and network layer.
pub struct Runner {
    queries: QueryManager,
    outgoing_queries_source: local_sync::channel::Receiver<OutgoingQuery>,
    incoming_msgs_source: mpsc::Receiver<(Message, SocketAddr)>,
}

impl Runner {
    pub async fn run(mut self) -> Result<(), Error> {
        loop {
            let next_timeout = self.queries.next_timeout();
            select! {
                biased;
                outgoing = self.outgoing_queries_source.next() => {
                    let query = outgoing.ok_or(Error::ChannelClosed)?;
                    self.queries.handle_one_outgoing(query).await?;
                }
                incoming = self.incoming_msgs_source.recv() => {
                    let msg = incoming.ok_or(Error::ChannelClosed)?;
                    self.queries.handle_one_incoming(msg).await?;
                }
                _ = Self::sleep_until(next_timeout), if next_timeout.is_some() => {
                    self.queries.handle_timeouts().await?;
                }
            }
        }
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
) -> (Client, Server, Runner) {
    let (outgoing_queries_sink, outgoing_queries_source) = local_sync::channel();
    let (incoming_queries_sink, incoming_queries_source) = local_sync::channel();

    let actor = Runner {
        queries: QueryManager::new(outgoing_msgs_sink, incoming_queries_sink),
        outgoing_queries_source,
        incoming_msgs_source,
    };
    (Client(outgoing_queries_sink), Server(incoming_queries_source), actor)
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
                log::debug!("[{}] => {:?}", remote_addr, $query_args);
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
        log::debug!("[{}] <= {:?}", self.source_addr, response);
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
            log::debug!("[{}] <= {:?}", self.source_addr, error_msg);
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
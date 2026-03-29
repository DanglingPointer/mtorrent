use crate::error::Error;
use crate::msgs::*;
use crate::u160::U160;
use derive_more::derive::From;
use local_async_utils::prelude::*;
use mtorrent_utils::trace_stopwatch;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;
use tokio::sync::mpsc;

#[cfg_attr(test, derive(Debug))]
#[derive(From)]
pub enum IncomingQuery {
    Ping(IncomingPingQuery),
    FindNode(IncomingFindNodeQuery),
    GetPeers(IncomingGetPeersQuery),
    AnnouncePeer(IncomingAnnouncePeerQuery),
}

#[cfg_attr(test, derive(Debug))]
pub struct IncomingGenericQuery<Q, R> {
    transaction_id: Vec<u8>,
    query: Q,
    response_sink: Option<mpsc::OwnedPermit<(Message, SocketAddr)>>,
    source_addr: SocketAddr,
    _stopwatch: Stopwatch,
    _response_type: PhantomData<R>,
}

pub type IncomingPingQuery = IncomingGenericQuery<PingArgs, PingResponse>;
pub type IncomingFindNodeQuery = IncomingGenericQuery<FindNodeArgs, FindNodeResponse>;
pub type IncomingGetPeersQuery = IncomingGenericQuery<GetPeersArgs, GetPeersResponse>;
pub type IncomingAnnouncePeerQuery = IncomingGenericQuery<AnnouncePeerArgs, AnnouncePeerResponse>;

impl IncomingQuery {
    pub(super) fn new(
        incoming: QueryMsg,
        tid: Vec<u8>,
        sink: mpsc::OwnedPermit<(Message, SocketAddr)>,
        remote_addr: SocketAddr,
    ) -> IncomingQuery {
        macro_rules! construct {
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
            QueryMsg::Ping(args) => construct!(args, "Ping"),
            QueryMsg::FindNode(args) => construct!(args, "FindNode"),
            QueryMsg::GetPeers(args) => construct!(args, "GetPeers"),
            QueryMsg::AnnouncePeer(args) => construct!(args, "AnnouncePeer"),
        }
    }

    pub fn node_id(&self) -> &U160 {
        match self {
            IncomingQuery::Ping(q) => &q.args().id,
            IncomingQuery::FindNode(q) => &q.args().id,
            IncomingQuery::GetPeers(q) => &q.args().id,
            IncomingQuery::AnnouncePeer(q) => &q.args().id,
        }
    }

    pub fn source_addr(&self) -> &SocketAddr {
        match self {
            IncomingQuery::Ping(q) => q.source_addr(),
            IncomingQuery::FindNode(q) => q.source_addr(),
            IncomingQuery::GetPeers(q) => q.source_addr(),
            IncomingQuery::AnnouncePeer(q) => q.source_addr(),
        }
    }
}

impl<Q, R> IncomingGenericQuery<Q, R> {
    pub fn args(&self) -> &Q {
        &self.query
    }

    pub fn source_addr(&self) -> &SocketAddr {
        &self.source_addr
    }

    pub fn respond(mut self, response: R) -> Result<(), Error>
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

    pub fn respond_error(mut self, error: ErrorMsg) -> Result<(), Error> {
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

use super::{OutboundQueries, OutgoingQuery};
use crate::error::Error;
use crate::msgs::*;
use local_async_utils::prelude::*;
use mtorrent_utils::trace_stopwatch;
use std::fmt::Debug;
use std::net::SocketAddr;
use tokio::sync::Semaphore;

/// Client for sending outgoing queries to different nodes.
pub(crate) struct QueryClient {
    channel: local_unbounded::Sender<OutgoingQuery>,
    query_slots: Semaphore,
}

impl QueryClient {
    pub(crate) fn new(
        OutboundQueries(channel): OutboundQueries,
        max_concurrent_queries: Option<usize>,
    ) -> Self {
        let query_slots = Semaphore::const_new(max_concurrent_queries.unwrap_or(0));
        if max_concurrent_queries.is_none() {
            query_slots.close();
        }
        Self {
            channel,
            query_slots,
        }
    }

    pub(crate) async fn ping(
        &self,
        destination: SocketAddr,
        query: PingArgs,
    ) -> Result<PingResponse, Error> {
        let _sw = trace_stopwatch!("Ping query to {destination}");
        self.do_query(destination, query).await
    }

    pub(crate) async fn find_node(
        &self,
        destination: SocketAddr,
        query: FindNodeArgs,
    ) -> Result<FindNodeResponse, Error> {
        let _sw = trace_stopwatch!("FindNode query to {destination}");
        self.do_query(destination, query).await
    }

    pub(crate) async fn get_peers(
        &self,
        destination: SocketAddr,
        query: GetPeersArgs,
    ) -> Result<GetPeersResponse, Error> {
        let _sw = trace_stopwatch!("GetPeers query to {destination}");
        self.do_query(destination, query).await
    }

    pub(crate) async fn announce_peer(
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

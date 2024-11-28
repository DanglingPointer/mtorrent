use super::error::Error;
use super::msgs::*;
use super::u160::U160;
use crate::utils::local_sync;
use crate::utils::stopwatch::Stopwatch;
use crate::{sec, trace_stopwatch};
use derive_more::derive::From;
use futures::StreamExt;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fmt::Debug;
use std::future::pending;
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;
use std::time::Duration;
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
    queries: details::QueryManager,
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
        queries: details::QueryManager::new(outgoing_msgs_sink, incoming_queries_sink),
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

// ------------------------------------------------------------------------------------------------

type ResponseSender = local_sync::oneshot::Sender<Result<ResponseMsg, Error>>;

struct OutgoingQuery {
    query: QueryMsg,
    destination_addr: SocketAddr,
    response_sink: ResponseSender,
}

// ------------------------------------------------------------------------------------------------

mod details {
    use super::*;

    struct PendingTimeout {
        timeout_at: Instant,
        tid: u16,
        retries_left: usize,
    }

    impl PartialEq for PendingTimeout {
        fn eq(&self, other: &Self) -> bool {
            self.timeout_at == other.timeout_at
        }
    }

    impl Eq for PendingTimeout {}

    impl PartialOrd for PendingTimeout {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for PendingTimeout {
        fn cmp(&self, other: &Self) -> Ordering {
            // reverse order for min-BinaryHeap
            other.timeout_at.cmp(&self.timeout_at)
        }
    }

    pub(super) struct QueryManager {
        next_tid: u16,
        outstanding_queries: HashMap<u16, OutgoingQuery>,
        pending_timeouts: BinaryHeap<PendingTimeout>,
        outgoing_msgs_sink: mpsc::Sender<(Message, SocketAddr)>,
        incoming_queries_sink: local_sync::channel::Sender<IncomingQuery>,
    }

    impl QueryManager {
        const RETRANSMIT_INTERVAL: Duration = sec!(1);
        const MAX_RETRANSMITS: usize = 4;

        pub(super) fn new(
            outgoing_msgs_sink: mpsc::Sender<(Message, SocketAddr)>,
            incoming_queries_sink: local_sync::channel::Sender<IncomingQuery>,
        ) -> Self {
            Self {
                next_tid: 1,
                outstanding_queries: Default::default(),
                pending_timeouts: Default::default(),
                outgoing_msgs_sink,
                incoming_queries_sink,
            }
        }

        pub(super) fn next_timeout(&self) -> Option<Instant> {
            self.pending_timeouts.peek().map(|pt| pt.timeout_at)
        }

        pub(super) async fn handle_timeouts(&mut self) -> Result<(), Error> {
            use std::collections::hash_map::Entry;

            while let Some(pending) = self.pending_timeouts.peek() {
                if pending.timeout_at > Instant::now() {
                    // the query hasn't timed out yet
                    break;
                }
                let mut pending = self.pending_timeouts.pop().unwrap();
                let mut outstanding = match self.outstanding_queries.entry(pending.tid) {
                    Entry::Occupied(occupied_entry) => occupied_entry,
                    Entry::Vacant(_) => panic!("no query for pending timeout"),
                };
                if pending.retries_left == 0 {
                    // erase entry and invoke callback with error
                    outstanding.remove().response_sink.send(Err(Error::Timeout));
                } else {
                    let tid = *outstanding.key();
                    let outstanding = outstanding.get_mut();
                    // retransmit query
                    let msg = Message {
                        transaction_id: tid.to_be_bytes().into(),
                        version: None,
                        data: MessageData::Query(outstanding.query.clone()),
                    };
                    self.outgoing_msgs_sink.send((msg, outstanding.destination_addr)).await?;
                    log::trace!(
                        "(RETRY) [{}] <= {:?}",
                        outstanding.destination_addr,
                        outstanding.query
                    );
                    // schedule next timeout
                    pending.timeout_at = Instant::now() + Self::RETRANSMIT_INTERVAL;
                    pending.retries_left -= 1;
                    self.pending_timeouts.push(pending);
                }
            }
            Ok(())
        }

        pub(super) async fn handle_one_outgoing(
            &mut self,
            query: OutgoingQuery,
        ) -> Result<(), Error> {
            let tid = self.next_tid;
            self.next_tid = tid.wrapping_add(1);

            let msg = Message {
                transaction_id: tid.to_be_bytes().into(),
                version: None,
                data: MessageData::Query(query.query.clone()),
            };
            match self.outgoing_msgs_sink.send((msg, query.destination_addr)).await {
                Ok(_) => {
                    self.outstanding_queries.insert(tid, query);
                    self.pending_timeouts.push(PendingTimeout {
                        timeout_at: Instant::now() + Self::RETRANSMIT_INTERVAL,
                        tid,
                        retries_left: Self::MAX_RETRANSMITS,
                    });
                    Ok(())
                }
                Err(e) => {
                    let _ = query.response_sink.send(Err(Error::ChannelClosed));
                    Err(e.into())
                }
            }
        }

        pub(super) async fn handle_one_incoming(
            &mut self,
            (msg, src_addr): (Message, SocketAddr),
        ) -> Result<(), Error> {
            if let MessageData::Query(request) = msg.data {
                let response_sink = self.outgoing_msgs_sink.clone().reserve_owned().await?;
                let incoming_query =
                    IncomingQuery::new(request, msg.transaction_id, response_sink, src_addr);
                self.incoming_queries_sink.send(incoming_query);
            } else if let Some((tid, outstanding)) =
                msg.transaction_id.last_chunk::<2>().and_then(|&tid| {
                    let tid = u16::from_be_bytes(tid);
                    self.outstanding_queries.remove(&tid).map(|handler| (tid, handler))
                })
            {
                // invoke callback with the received response
                let _ = match msg.data {
                    MessageData::Response(response) => outstanding.response_sink.send(Ok(response)),
                    MessageData::Error(error) => outstanding.response_sink.send(Err(error.into())),
                    MessageData::Query(_) => unreachable!(),
                };
                // erase the corresponding timeout
                self.pending_timeouts.retain(|pt| pt.tid != tid);
            } else {
                log::warn!(
                    "Received orphaned {} message from {}",
                    match msg.data {
                        MessageData::Response(_) => "response",
                        MessageData::Error(_) => "error",
                        MessageData::Query(_) => unreachable!(),
                    },
                    src_addr
                );
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::millisec;
    use std::cell::Cell;
    use std::iter;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use tokio::task::{self, yield_now};
    use tokio::time::sleep;
    use tokio_test::task::spawn;
    use tokio_test::{assert_pending, assert_ready};

    thread_local! {
        // To avoid having to add #[tokio::test] to every test, we need a way to disable calls to tokio::time::sleep(),
        // (which would panic without a tokio runtime).
        pub static SLEEP_ENABLED: Cell<bool> = const { Cell::new(false) };
    }

    const IP: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 12345));

    #[test]
    fn test_outgoing_ping_success() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut ping_fut = spawn(client.ping(
            IP,
            PingArgs {
                id: [1u8; 20].into(),
            },
        ));
        assert_pending!(ping_fut.poll());

        assert_pending!(runner_fut.poll());
        let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_ping.transaction_id, vec![0u8, 1u8]);
        assert_eq!(outgoing_ping.version, None);
        if let MessageData::Query(QueryMsg::Ping(args)) = outgoing_ping.data {
            assert_eq!(args.id, [1u8; 20].into());
        } else {
            panic!("outgoing message has incorrect type");
        }

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: outgoing_ping.transaction_id,
                    version: None,
                    data: MessageData::Response(
                        PingResponse {
                            id: [2u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert!(runner_fut.is_woken());
        assert_pending!(runner_fut.poll());
        assert!(ping_fut.is_woken());
        let ping_result = assert_ready!(ping_fut.poll());
        let ping_response = ping_result.unwrap();
        assert_eq!(ping_response.id, [2u8; 20].into());
    }

    #[test]
    fn test_outgoing_find_node_success() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut find_node_fut = spawn(client.find_node(
            IP,
            FindNodeArgs {
                id: [1u8; 20].into(),
                target: [2u8; 20].into(),
            },
        ));
        assert_pending!(find_node_fut.poll());

        assert_pending!(runner_fut.poll());
        let (outgoing_find_node, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_find_node.transaction_id, vec![0u8, 1u8]);
        assert_eq!(outgoing_find_node.version, None);
        if let MessageData::Query(QueryMsg::FindNode(args)) = outgoing_find_node.data {
            assert_eq!(args.id, [1u8; 20].into());
            assert_eq!(args.target, [2u8; 20].into());
        } else {
            panic!("outgoing message has incorrect type");
        }

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: outgoing_find_node.transaction_id,
                    version: None,
                    data: MessageData::Response(
                        FindNodeResponse {
                            id: [3u8; 20].into(),
                            nodes: vec![(
                                [4u8; 20].into(),
                                SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234),
                            )],
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let find_node_result = assert_ready!(find_node_fut.poll());
        let find_node_response = find_node_result.unwrap();
        assert_eq!(find_node_response.id, [3u8; 20].into());
        assert_eq!(
            find_node_response.nodes,
            vec![([4u8; 20].into(), SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234))]
        );
    }

    #[test]
    fn test_outgoing_get_peers_success() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut get_peers_fut = spawn(client.get_peers(
            IP,
            GetPeersArgs {
                id: [1u8; 20].into(),
                info_hash: [2u8; 20].into(),
            },
        ));
        assert_pending!(get_peers_fut.poll());

        assert_pending!(runner_fut.poll());
        let (outgoing_get_peers, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_get_peers.transaction_id, vec![0u8, 1u8]);
        assert_eq!(outgoing_get_peers.version, None);
        if let MessageData::Query(QueryMsg::GetPeers(args)) = outgoing_get_peers.data {
            assert_eq!(args.id, [1u8; 20].into());
            assert_eq!(args.info_hash, [2u8; 20].into());
        } else {
            panic!("outgoing message has incorrect type");
        }

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: outgoing_get_peers.transaction_id,
                    version: None,
                    data: MessageData::Response(
                        GetPeersResponse {
                            id: [3u8; 20].into(),
                            token: vec![4u8; 2],
                            data: GetPeersResponseData::Peers(vec![SocketAddrV4::new(
                                Ipv4Addr::LOCALHOST,
                                1234,
                            )]),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let get_peers_result = assert_ready!(get_peers_fut.poll());
        let get_peers_response = get_peers_result.unwrap();
        assert_eq!(get_peers_response.id, [3u8; 20].into());
        assert_eq!(get_peers_response.token, vec![4u8; 2]);
        assert!(
            matches!(get_peers_response.data, GetPeersResponseData::Peers(peers) if peers == vec![SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                1234,
            )])
        );
    }

    #[test]
    fn test_outgoing_annouce_peer_success() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut announce_peer_fut = spawn(client.announce_peer(
            IP,
            AnnouncePeerArgs {
                id: [1u8; 20].into(),
                info_hash: [2u8; 20].into(),
                port: Some(1234),
                token: vec![3u8; 2],
            },
        ));
        assert_pending!(announce_peer_fut.poll());

        assert_pending!(runner_fut.poll());
        let (outgoing_announce_peer, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_announce_peer.transaction_id, vec![0u8, 1u8]);
        assert_eq!(outgoing_announce_peer.version, None);
        if let MessageData::Query(QueryMsg::AnnouncePeer(args)) = outgoing_announce_peer.data {
            assert_eq!(args.id, [1u8; 20].into());
            assert_eq!(args.info_hash, [2u8; 20].into());
            assert_eq!(args.port, Some(1234));
            assert_eq!(args.token, vec![3u8; 2]);
        } else {
            panic!("outgoing message has incorrect type");
        }

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: outgoing_announce_peer.transaction_id,
                    version: None,
                    data: MessageData::Response(
                        AnnouncePeerResponse {
                            id: [3u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let announce_peer_result = assert_ready!(announce_peer_fut.poll());
        let announce_peer_response = announce_peer_result.unwrap();
        assert_eq!(announce_peer_response.id, [3u8; 20].into());
    }

    #[test]
    fn test_concurrent_outgoing_queries_out_of_order() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        // start ping
        let mut ping_fut = spawn(client.ping(
            IP,
            PingArgs {
                id: [1u8; 20].into(),
            },
        ));
        assert_pending!(ping_fut.poll());

        // start announce peer
        let mut announce_peer_fut = spawn(client.announce_peer(
            IP,
            AnnouncePeerArgs {
                id: [1u8; 20].into(),
                info_hash: [2u8; 20].into(),
                port: Some(1234),
                token: vec![3u8; 2],
            },
        ));
        assert_pending!(announce_peer_fut.poll());

        // verify ping query
        assert_pending!(runner_fut.poll());
        let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_ping.transaction_id, vec![0u8, 1u8]);
        assert_eq!(outgoing_ping.version, None);
        if let MessageData::Query(QueryMsg::Ping(args)) = outgoing_ping.data {
            assert_eq!(args.id, [1u8; 20].into());
        } else {
            panic!("outgoing message has incorrect type");
        }

        // verify announce peer
        assert_pending!(announce_peer_fut.poll());
        let (outgoing_announce_peer, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_announce_peer.transaction_id, vec![0u8, 2u8]);
        assert_eq!(outgoing_announce_peer.version, None);
        if let MessageData::Query(QueryMsg::AnnouncePeer(args)) = outgoing_announce_peer.data {
            assert_eq!(args.id, [1u8; 20].into());
            assert_eq!(args.info_hash, [2u8; 20].into());
            assert_eq!(args.port, Some(1234));
            assert_eq!(args.token, vec![3u8; 2]);
        } else {
            panic!("outgoing message has incorrect type");
        }

        // respond to announce peer
        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: outgoing_announce_peer.transaction_id,
                    version: None,
                    data: MessageData::Response(
                        AnnouncePeerResponse {
                            id: [3u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert!(runner_fut.is_woken());
        assert_pending!(runner_fut.poll());
        assert_pending!(ping_fut.poll());
        let announce_peer_result = assert_ready!(announce_peer_fut.poll());
        let announce_peer_response = announce_peer_result.unwrap();
        assert_eq!(announce_peer_response.id, [3u8; 20].into());

        // respond to ping
        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: outgoing_ping.transaction_id,
                    version: None,
                    data: MessageData::Response(
                        PingResponse {
                            id: [2u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert!(runner_fut.is_woken());
        assert_pending!(runner_fut.poll());
        let ping_result = assert_ready!(ping_fut.poll());
        let ping_response = ping_result.unwrap();
        assert_eq!(ping_response.id, [2u8; 20].into());
    }

    #[tokio::test(start_paused = true)]
    async fn test_outgoing_query_retransmissions() {
        task::LocalSet::new()
            .run_until(async {
                SLEEP_ENABLED.with(|sleep_enabled| sleep_enabled.set(true));
                let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
                let (_incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
                let (client, _server, runner) =
                    setup_routing(outgoing_msgs_sink, incoming_msgs_source);

                task::spawn_local(runner.run());

                // start ping query
                let mut ping_fut = spawn(client.ping(
                    IP,
                    PingArgs {
                        id: [1u8; 20].into(),
                    },
                ));
                assert_pending!(ping_fut.poll());

                // verify ping sent out on the network
                yield_now().await;
                let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
                assert_eq!(outgoing_ping.transaction_id, vec![0u8, 1u8]);
                assert!(matches!(outgoing_ping.data, MessageData::Query(QueryMsg::Ping(_))));

                for _ in 0..4 {
                    sleep(sec!(1)).await;
                    yield_now().await;
                    let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
                    assert_eq!(outgoing_ping.transaction_id, vec![0u8, 1u8]);
                    assert!(matches!(outgoing_ping.data, MessageData::Query(QueryMsg::Ping(_))));
                    assert_pending!(ping_fut.poll());
                }

                sleep(sec!(1)).await;
                yield_now().await;
                assert!(outgoing_msgs_source.try_recv().is_err());
                let ping_result = assert_ready!(ping_fut.poll());
                let ping_error = ping_result.unwrap_err();
                assert!(matches!(ping_error, Error::Timeout));
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_outgoing_concurrent_retransmissions() {
        // let _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Trace).init();
        task::LocalSet::new()
            .run_until(async {
                SLEEP_ENABLED.with(|sleep_enabled| sleep_enabled.set(true));
                let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
                let (_incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
                let (client, _server, runner) =
                    setup_routing(outgoing_msgs_sink, incoming_msgs_source);

                macro_rules! verify_msg_count {
                    ($ping_count:expr, $get_peers_count:expr) => {{
                        yield_now().await;
                        let msgs: Vec<Message> = iter::from_fn(|| {
                            outgoing_msgs_source.try_recv().map(|(msg, _addr)| msg).ok()
                        })
                        .collect();
                        assert_eq!(msgs.len(), $ping_count + $get_peers_count);
                        assert_eq!(
                            msgs.iter()
                                .filter(|msg| matches!(
                                    msg.data,
                                    MessageData::Query(QueryMsg::Ping(_))
                                ) && msg.transaction_id == vec![0u8, 1u8])
                                .count(),
                            $ping_count
                        );
                        assert_eq!(
                            msgs.iter()
                                .filter(|msg| matches!(
                                    msg.data,
                                    MessageData::Query(QueryMsg::GetPeers(_))
                                ) && msg.transaction_id == vec![0u8, 2u8])
                                .count(),
                            $get_peers_count
                        );
                    }};
                }

                task::spawn_local(runner.run());

                // start ping query
                let mut ping_fut = spawn(client.ping(
                    IP,
                    PingArgs {
                        id: [1u8; 20].into(),
                    },
                ));
                assert_pending!(ping_fut.poll());

                // verify ping sent out on the network
                verify_msg_count!(1, 0);

                // start get peers 3s later
                sleep(sec!(3)).await;
                let mut get_peers_fut = spawn(client.get_peers(
                    IP,
                    GetPeersArgs {
                        id: [1u8; 20].into(),
                        info_hash: [2u8; 20].into(),
                    },
                ));
                assert_pending!(get_peers_fut.poll());

                // verify get peers sent out on the network + 3 ping retransmissions
                verify_msg_count!(3, 1);

                // ping times out first
                assert_pending!(ping_fut.poll());
                sleep(sec!(2)).await;
                yield_now().await;
                let ping_result = assert_ready!(ping_fut.poll());
                let ping_error = ping_result.unwrap_err();
                assert!(matches!(ping_error, Error::Timeout));
                assert_pending!(get_peers_fut.poll());

                // get peers times out 3s later
                sleep(sec!(3)).await;
                yield_now().await;
                let get_peers_result = assert_ready!(get_peers_fut.poll());
                let get_peers_error = get_peers_result.unwrap_err();
                assert!(matches!(get_peers_error, Error::Timeout));

                // verify 1 ping retransmit + 4 get peers retransmits
                sleep(sec!(1)).await;
                verify_msg_count!(1, 4);
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_outgoing_queries_timer_cleanup() {
        // let _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Trace).init();
        task::LocalSet::new()
            .run_until(async {
                SLEEP_ENABLED.with(|sleep_enabled| sleep_enabled.set(true));
                let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
                let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
                let (client, _server, runner) =
                    setup_routing(outgoing_msgs_sink, incoming_msgs_source);

                macro_rules! verify_msg_count {
                    ($ping_count:expr, $get_peers_count:expr) => {{
                        yield_now().await;
                        let msgs: Vec<Message> = iter::from_fn(|| {
                            outgoing_msgs_source.try_recv().map(|(msg, _addr)| msg).ok()
                        })
                        .collect();
                        assert_eq!(msgs.len(), $ping_count + $get_peers_count);
                        assert_eq!(
                            msgs.iter()
                                .filter(|msg| matches!(
                                    msg.data,
                                    MessageData::Query(QueryMsg::Ping(_))
                                ) && msg.transaction_id == vec![0u8, 1u8])
                                .count(),
                            $ping_count
                        );
                        assert_eq!(
                            msgs.iter()
                                .filter(|msg| matches!(
                                    msg.data,
                                    MessageData::Query(QueryMsg::GetPeers(_))
                                ) && msg.transaction_id == vec![0u8, 2u8])
                                .count(),
                            $get_peers_count
                        );
                    }};
                }

                task::spawn_local(runner.run());

                // start ping query
                let mut ping_fut = spawn(client.ping(
                    IP,
                    PingArgs {
                        id: [1u8; 20].into(),
                    },
                ));
                assert_pending!(ping_fut.poll());

                // verify ping sent out on the network
                yield_now().await;
                let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
                assert!(matches!(outgoing_ping.data, MessageData::Query(QueryMsg::Ping(_))));

                // start get peers 500ms later
                sleep(millisec!(500)).await;
                let mut get_peers_fut = spawn(client.get_peers(
                    IP,
                    GetPeersArgs {
                        id: [1u8; 20].into(),
                        info_hash: [2u8; 20].into(),
                    },
                ));
                assert_pending!(get_peers_fut.poll());

                // verify get peers sent out on the network
                yield_now().await;
                let (outgoing_get_peers, _) = outgoing_msgs_source.try_recv().unwrap();
                assert!(matches!(
                    outgoing_get_peers.data,
                    MessageData::Query(QueryMsg::GetPeers(_))
                ));

                // 1 retransmission of each
                sleep(millisec!(1500)).await;
                verify_msg_count!(2, 1);

                // respond to get peers
                incoming_msgs_sink
                    .try_send((
                        Message {
                            transaction_id: outgoing_get_peers.transaction_id,
                            version: None,
                            data: MessageData::Response(
                                GetPeersResponse {
                                    id: [69u8; 20].into(),
                                    token: vec![3u8; 2],
                                    data: GetPeersResponseData::Peers(vec![]),
                                }
                                .into(),
                            ),
                        },
                        IP,
                    ))
                    .unwrap();
                yield_now().await;
                let get_peers_result = assert_ready!(get_peers_fut.poll());
                let get_peers_response = get_peers_result.unwrap();
                assert_eq!(get_peers_response.id, [69u8; 20].into());

                for _ in 0..2 {
                    sleep(sec!(1)).await;
                    verify_msg_count!(1, 0);
                }

                // ping times out
                sleep(millisec!(1000)).await;
                assert_pending!(ping_fut.poll());
                yield_now().await;
                let ping_result = assert_ready!(ping_fut.poll());
                let ping_error = ping_result.unwrap_err();
                assert!(matches!(ping_error, Error::Timeout));
            })
            .await;
    }

    #[test]
    fn test_outgoing_ping_error_response() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut ping_fut = spawn(client.ping(
            IP,
            PingArgs {
                id: [1u8; 20].into(),
            },
        ));
        assert_pending!(ping_fut.poll());

        assert_pending!(runner_fut.poll());
        let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_ping.transaction_id, vec![0u8, 1u8]);
        assert_eq!(outgoing_ping.version, None);
        if let MessageData::Query(QueryMsg::Ping(args)) = outgoing_ping.data {
            assert_eq!(args.id, [1u8; 20].into());
        } else {
            panic!("outgoing message has incorrect type");
        }

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: outgoing_ping.transaction_id,
                    version: None,
                    data: MessageData::Error(ErrorMsg {
                        error_code: ErrorCode::Generic,
                        error_msg: "Something went wrong".to_string(),
                    }),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let ping_result = assert_ready!(ping_fut.poll());
        let ping_error = ping_result.unwrap_err();
        if let Error::ErrorResponse(ping_error) = ping_error {
            assert_eq!(ping_error.error_code, ErrorCode::Generic);
            assert_eq!(ping_error.error_msg, "Something went wrong");
        } else {
            panic!("unexpected error type");
        }
    }

    #[test]
    fn test_outgoing_ping_channel_error() {
        let (outgoing_msgs_sink, outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut ping_fut = spawn(client.ping(
            IP,
            PingArgs {
                id: [1u8; 20].into(),
            },
        ));
        assert_pending!(ping_fut.poll());

        drop(outgoing_msgs_source);
        drop(incoming_msgs_sink);
        assert_ready!(runner_fut.poll()).unwrap_err();
        let ping_result = assert_ready!(ping_fut.poll());
        let ping_error = ping_result.unwrap_err();
        assert!(matches!(ping_error, Error::ChannelClosed));
    }

    #[test]
    fn test_incoming_ping_success() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut incoming_query_fut = spawn(server.0);
        assert_pending!(incoming_query_fut.poll_next());
        assert_pending!(runner_fut.poll());

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![123u8, 234u8],
                    version: None,
                    data: MessageData::Query(
                        PingArgs {
                            id: [1u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_ping = match incoming_query {
            Some(IncomingQuery::Ping(ping)) => ping,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_ping.args().id, [1u8; 20].into());
        assert_eq!(incoming_ping.source_addr(), &IP);

        incoming_ping
            .respond(PingResponse {
                id: [2u8; 20].into(),
            })
            .unwrap();
        assert_pending!(runner_fut.poll());
        let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_response.transaction_id, vec![123u8, 234u8]);
        assert_eq!(outgoing_response.version, None);
        let ping_response: PingResponse = match outgoing_response.data {
            MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
            _ => panic!("outgoing message is not a response"),
        };
        assert_eq!(ping_response.id, [2u8; 20].into());
    }

    #[test]
    fn test_incoming_find_node_success() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut incoming_query_fut = spawn(server.0);
        assert_pending!(incoming_query_fut.poll_next());
        assert_pending!(runner_fut.poll());

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![234u8, 123u8],
                    version: None,
                    data: MessageData::Query(
                        FindNodeArgs {
                            id: [1u8; 20].into(),
                            target: [2u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_find_node = match incoming_query {
            Some(IncomingQuery::FindNode(find_node)) => find_node,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_find_node.args().id, [1u8; 20].into());
        assert_eq!(incoming_find_node.args().target, [2u8; 20].into());

        incoming_find_node
            .respond(FindNodeResponse {
                id: [3u8; 20].into(),
                nodes: vec![([4u8; 20].into(), SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234))],
            })
            .unwrap();
        assert_pending!(runner_fut.poll());
        let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
        assert_eq!(outgoing_response.version, None);
        let find_node_response: FindNodeResponse = match outgoing_response.data {
            MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
            _ => panic!("outgoing message is not a response"),
        };
        assert_eq!(find_node_response.id, [3u8; 20].into());
        assert_eq!(
            find_node_response.nodes,
            vec![([4u8; 20].into(), SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234))]
        );
    }

    #[test]
    fn test_incoming_get_peers_success() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut incoming_query_fut = spawn(server.0);
        assert_pending!(incoming_query_fut.poll_next());
        assert_pending!(runner_fut.poll());

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![234u8, 123u8],
                    version: None,
                    data: MessageData::Query(
                        GetPeersArgs {
                            id: [1u8; 20].into(),
                            info_hash: [2u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_get_peers = match incoming_query {
            Some(IncomingQuery::GetPeers(get_peers)) => get_peers,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_get_peers.args().id, [1u8; 20].into());
        assert_eq!(incoming_get_peers.args().info_hash, [2u8; 20].into());

        incoming_get_peers
            .respond(GetPeersResponse {
                id: [3u8; 20].into(),
                token: vec![4u8; 2],
                data: GetPeersResponseData::Peers(vec![SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    1234,
                )]),
            })
            .unwrap();
        assert_pending!(runner_fut.poll());
        let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
        assert_eq!(outgoing_response.version, None);
        let get_peers_response: GetPeersResponse = match outgoing_response.data {
            MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
            _ => panic!("outgoing message is not a response"),
        };
        assert_eq!(get_peers_response.id, [3u8; 20].into());
        assert_eq!(get_peers_response.token, vec![4u8; 2]);
        assert!(
            matches!(get_peers_response.data, GetPeersResponseData::Peers(peers) if peers == vec![SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                1234,
            )])
        );
    }

    #[test]
    fn test_incoming_announce_peer_success() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut incoming_query_fut = spawn(server.0);
        assert_pending!(incoming_query_fut.poll_next());
        assert_pending!(runner_fut.poll());

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![234u8, 123u8],
                    version: None,
                    data: MessageData::Query(
                        AnnouncePeerArgs {
                            id: [1u8; 20].into(),
                            info_hash: [2u8; 20].into(),
                            port: Some(1234),
                            token: vec![3u8; 2],
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_announce_peer = match incoming_query {
            Some(IncomingQuery::AnnouncePeer(announce_peer)) => announce_peer,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_announce_peer.args().id, [1u8; 20].into());
        assert_eq!(incoming_announce_peer.args().info_hash, [2u8; 20].into());
        assert_eq!(incoming_announce_peer.args().port, Some(1234));
        assert_eq!(incoming_announce_peer.args().token, vec![3u8; 2]);

        incoming_announce_peer
            .respond(AnnouncePeerResponse {
                id: [4u8; 20].into(),
            })
            .unwrap();
        assert_pending!(runner_fut.poll());
        let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
        assert_eq!(outgoing_response.version, None);
        let announce_peer_response: AnnouncePeerResponse = match outgoing_response.data {
            MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
            _ => panic!("outgoing message is not a response"),
        };
        assert_eq!(announce_peer_response.id, [4u8; 20].into());
    }

    #[test]
    fn test_concurrent_incoming_queries_out_of_order() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut incoming_query_fut = spawn(server.0);
        assert_pending!(incoming_query_fut.poll_next());
        assert_pending!(runner_fut.poll());

        // incoming find node
        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![234u8, 123u8],
                    version: None,
                    data: MessageData::Query(
                        FindNodeArgs {
                            id: [1u8; 20].into(),
                            target: [2u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_find_node = match incoming_query {
            Some(IncomingQuery::FindNode(find_node)) => find_node,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_find_node.args().id, [1u8; 20].into());
        assert_eq!(incoming_find_node.args().target, [2u8; 20].into());

        // incoming ping
        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![234u8, 124u8],
                    version: None,
                    data: MessageData::Query(
                        PingArgs {
                            id: [3u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_ping = match incoming_query {
            Some(IncomingQuery::Ping(ping)) => ping,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_ping.args().id, [3u8; 20].into());

        // respond to ping
        incoming_ping
            .respond(PingResponse {
                id: [2u8; 20].into(),
            })
            .unwrap();
        assert_pending!(runner_fut.poll());
        let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_response.transaction_id, vec![234u8, 124u8]);
        assert_eq!(outgoing_response.version, None);
        let ping_response: PingResponse = match outgoing_response.data {
            MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
            _ => panic!("outgoing message is not a response"),
        };
        assert_eq!(ping_response.id, [2u8; 20].into());

        // respond to find node
        incoming_find_node
            .respond(FindNodeResponse {
                id: [3u8; 20].into(),
                nodes: vec![],
            })
            .unwrap();
        assert_pending!(runner_fut.poll());
        let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
        assert_eq!(outgoing_response.version, None);
        let find_node_response: FindNodeResponse = match outgoing_response.data {
            MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
            _ => panic!("outgoing message is not a response"),
        };
        assert_eq!(find_node_response.id, [3u8; 20].into());
        assert!(find_node_response.nodes.is_empty());
    }

    #[test]
    fn test_incoming_ping_error_response() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut incoming_query_fut = spawn(server.0);
        assert_pending!(incoming_query_fut.poll_next());
        assert_pending!(runner_fut.poll());

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![234u8, 123u8],
                    version: None,
                    data: MessageData::Query(
                        PingArgs {
                            id: [1u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_ping = match incoming_query {
            Some(IncomingQuery::Ping(ping)) => ping,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_ping.args().id, [1u8; 20].into());

        incoming_ping
            .respond_error(ErrorMsg {
                error_code: ErrorCode::Generic,
                error_msg: "Something went wrong".to_string(),
            })
            .unwrap();
        assert_pending!(runner_fut.poll());
        let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
        assert_eq!(outgoing_response.version, None);
        let ping_error: ErrorMsg = match outgoing_response.data {
            MessageData::Error(error_msg) => error_msg,
            _ => panic!("outgoing message is not an error"),
        };
        assert_eq!(ping_error.error_code, ErrorCode::Generic);
        assert_eq!(ping_error.error_msg, "Something went wrong");
    }

    #[test]
    fn test_incoming_ping_error_response_when_dropped() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut incoming_query_fut = spawn(server.0);
        assert_pending!(incoming_query_fut.poll_next());
        assert_pending!(runner_fut.poll());

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![234u8, 123u8],
                    version: None,
                    data: MessageData::Query(
                        PingArgs {
                            id: [1u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_ping = match incoming_query {
            Some(IncomingQuery::Ping(ping)) => ping,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_ping.args().id, [1u8; 20].into());

        drop(incoming_ping);
        assert_pending!(runner_fut.poll());
        let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
        assert_eq!(outgoing_response.version, None);
        let ping_error: ErrorMsg = match outgoing_response.data {
            MessageData::Error(error_msg) => error_msg,
            _ => panic!("outgoing message is not an error"),
        };
        assert_eq!(ping_error.error_code, ErrorCode::Server);
        assert_eq!(ping_error.error_msg, "Unable to handle query");
    }

    #[test]
    fn test_incoming_ping_channel_error() {
        let (outgoing_msgs_sink, outgoing_msgs_source) = mpsc::channel(8);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
        let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
        let mut runner_fut = spawn(runner.run());

        let mut incoming_query_fut = spawn(server.0);
        assert_pending!(incoming_query_fut.poll_next());
        assert_pending!(runner_fut.poll());

        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![234u8, 123u8],
                    version: None,
                    data: MessageData::Query(
                        PingArgs {
                            id: [1u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(runner_fut.poll());
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_ping = match incoming_query {
            Some(IncomingQuery::Ping(ping)) => ping,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_ping.args().id, [1u8; 20].into());

        drop(outgoing_msgs_source);
        drop(incoming_msgs_sink);
        assert_ready!(runner_fut.poll()).unwrap_err();
        let error = incoming_ping
            .respond(PingResponse {
                id: [2u8; 20].into(),
            })
            .unwrap_err();
        assert!(matches!(error, Error::ChannelClosed));
    }

    #[test]
    fn test_router_prioritizes_outgoing_messages() {
        let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(1);
        let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(1);
        let (client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);

        let mut runner_fut = spawn(runner.run());
        let mut incoming_query_fut = spawn(server.0);

        // enqueue incoming ping
        incoming_msgs_sink
            .try_send((
                Message {
                    transaction_id: vec![234u8, 123u8],
                    version: None,
                    data: MessageData::Query(
                        PingArgs {
                            id: [1u8; 20].into(),
                        }
                        .into(),
                    ),
                },
                IP,
            ))
            .unwrap();
        assert_pending!(incoming_query_fut.poll_next());

        // start outgoing ping
        let mut ping_fut = spawn(client.ping(
            IP,
            PingArgs {
                id: [1u8; 20].into(),
            },
        ));
        assert_pending!(ping_fut.poll());

        // poll router once
        assert_pending!(runner_fut.poll());

        // verify outgoing ping and no incoming ping
        let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
        assert_eq!(outgoing_ping.transaction_id, vec![0u8, 1u8]);
        assert_eq!(outgoing_ping.version, None);
        if let MessageData::Query(QueryMsg::Ping(args)) = outgoing_ping.data {
            assert_eq!(args.id, [1u8; 20].into());
        } else {
            panic!("outgoing message has incorrect type");
        }
        assert_pending!(incoming_query_fut.poll_next());

        // poll router once more
        assert_pending!(runner_fut.poll());

        // verify incoming ping
        let incoming_query = assert_ready!(incoming_query_fut.poll_next());
        let incoming_ping = match incoming_query {
            Some(IncomingQuery::Ping(ping)) => ping,
            Some(_) => panic!("incoming message has wrong type"),
            None => panic!("channel closed"),
        };
        assert_eq!(incoming_ping.args().id, [1u8; 20].into());
    }
}

use super::IncomingQuery;
use crate::dht::error::Error;
use crate::dht::msgs::*;
use local_async_utils::local_sync;
use local_async_utils::millisec;
use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub(super) struct OutgoingQuery {
    pub(super) query: QueryMsg,
    pub(super) destination_addr: SocketAddr,
    pub(super) response_sink: local_sync::oneshot::Sender<Result<ResponseMsg, Error>>,
}

struct PendingTimeout {
    timeout_at: Instant,
    tid: u32,
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

pub struct QueryManager {
    next_tid: u32,
    outstanding_queries: HashMap<u32, OutgoingQuery>,
    pending_timeouts: BinaryHeap<PendingTimeout>,
    outgoing_msgs_sink: mpsc::Sender<(Message, SocketAddr)>,
    incoming_queries_sink: local_sync::channel::Sender<IncomingQuery>,
}

impl QueryManager {
    const INITIAL_RTO: Duration = millisec!(1500); // as per rfc8489 section-6.2.1
    const MAX_RETRANSMITS: usize = 2;

    pub fn new(
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

    pub fn next_timeout(&self) -> Option<Instant> {
        self.pending_timeouts.peek().map(|pt| pt.timeout_at)
    }

    pub async fn handle_timeouts(&mut self) -> Result<(), Error> {
        loop {
            let mut timeout = match self.pending_timeouts.peek_mut() {
                Some(timeout) if timeout.timeout_at <= Instant::now() => PeekMut::pop(timeout),
                _ => break,
            };
            let mut outstanding = match self.outstanding_queries.entry(timeout.tid) {
                Entry::Occupied(occupied_entry) => occupied_entry,
                Entry::Vacant(_) => unreachable!("no query for pending timeout"),
            };
            if timeout.retries_left == 0 {
                // erase entry and invoke callback with error
                outstanding.remove().response_sink.send(Err(Error::Timeout));
            } else {
                let query = outstanding.get_mut();
                // retransmit query
                let msg = Message {
                    transaction_id: timeout.tid.to_be_bytes().into(),
                    version: None,
                    data: MessageData::Query(query.query.clone()),
                };
                self.outgoing_msgs_sink.send((msg, query.destination_addr)).await?;
                let retry_index = Self::MAX_RETRANSMITS - timeout.retries_left;
                log::trace!(
                    "(RETRY {}) [{}] <= {:?}",
                    retry_index,
                    query.destination_addr,
                    query.query
                );
                // schedule next timeout
                timeout.retries_left -= 1;
                let next_rto = Self::INITIAL_RTO * (2 << retry_index) as u32;
                timeout.timeout_at = Instant::now() + next_rto;
                self.pending_timeouts.push(timeout);
            }
        }
        Ok(())
    }

    pub async fn handle_one_outgoing(&mut self, query: OutgoingQuery) -> Result<(), Error> {
        assert!((self.outstanding_queries.len() as u32) < u32::MAX, "infinite loop");
        let tid = loop {
            let tid = self.next_tid;
            self.next_tid = tid.wrapping_add(1);
            if !self.outstanding_queries.contains_key(&tid) {
                break tid;
            }
        };

        let msg = Message {
            transaction_id: tid.to_be_bytes().into(),
            version: None,
            data: MessageData::Query(query.query.clone()),
        };
        match self.outgoing_msgs_sink.send((msg, query.destination_addr)).await {
            Ok(_) => {
                self.outstanding_queries.insert(tid, query);
                self.pending_timeouts.push(PendingTimeout {
                    timeout_at: Instant::now() + Self::INITIAL_RTO,
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

    pub async fn handle_one_incoming(
        &mut self,
        (msg, src_addr): (Message, SocketAddr),
    ) -> Result<(), Error> {
        if let MessageData::Query(request) = msg.data {
            let response_sink = self.outgoing_msgs_sink.clone().reserve_owned().await?;
            let incoming_query =
                IncomingQuery::new(request, msg.transaction_id, response_sink, src_addr);
            self.incoming_queries_sink.send(incoming_query);
        } else if let Some((tid, outstanding)) =
            msg.transaction_id.last_chunk::<4>().and_then(|&tid| {
                let tid = u32::from_be_bytes(tid);
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

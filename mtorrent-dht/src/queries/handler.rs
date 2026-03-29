use super::{IncomingQuery, OutgoingQuery};
use crate::error::Error;
use crate::msgs::*;
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::time::{DelayQueue, delay_queue};

pub struct Handler {
    next_tid: u32,
    outstanding_queries: DelayQueue<(OutgoingQuery, u32)>,
    tid_to_key: HashMap<u32, delay_queue::Key>,

    outgoing_msgs_sink: mpsc::Sender<(Message, SocketAddr)>,
    incoming_queries_sink: local_unbounded::Sender<IncomingQuery>,
}

impl Handler {
    pub(super) const TIMEOUT: Duration = sec!(2);

    pub fn new(
        outgoing_msgs_sink: mpsc::Sender<(Message, SocketAddr)>,
        incoming_queries_sink: local_unbounded::Sender<IncomingQuery>,
    ) -> Self {
        Self {
            next_tid: 1,
            outstanding_queries: Default::default(),
            tid_to_key: Default::default(),
            outgoing_msgs_sink,
            incoming_queries_sink,
        }
    }

    pub async fn handle_outgoing(&mut self, query: OutgoingQuery) -> Result<(), Error> {
        let tid = self.next_tid;
        self.next_tid = tid.wrapping_add(1);

        let msg = Message {
            transaction_id: tid.to_be_bytes().into(),
            version: None,
            data: MessageData::Query(query.query.clone()),
        };
        match self.outgoing_msgs_sink.send((msg, query.destination_addr)).await {
            Ok(_) => {
                let queue_key = self.outstanding_queries.insert((query, tid), Self::TIMEOUT);
                self.tid_to_key.insert(tid, queue_key);
                Ok(())
            }
            Err(e) => {
                let _ = query.response_sink.send(Err(Error::ChannelClosed));
                Err(e.into())
            }
        }
    }

    pub async fn handle_incoming(
        &mut self,
        (msg, src_addr): (Message, SocketAddr),
    ) -> Result<(), Error> {
        let query_result = match msg.data {
            MessageData::Query(request) => {
                let response_sink = self.outgoing_msgs_sink.clone().reserve_owned().await?;
                let incoming_query =
                    IncomingQuery::new(request, msg.transaction_id, response_sink, src_addr);
                self.incoming_queries_sink.send(incoming_query)?;
                return Ok(());
            }
            MessageData::Response(response_msg) => Ok(response_msg),
            MessageData::Error(error_msg) => Err(error_msg.into()),
        };

        if let Some(tid) = msg.transaction_id.last_chunk::<4>().cloned().map(u32::from_be_bytes)
            && let Some(queue_key) = self.tid_to_key.remove(&tid)
        {
            let (outstanding, _tid) = self.outstanding_queries.remove(&queue_key).into_inner();
            _ = outstanding.response_sink.send(query_result);
        }

        Ok(())
    }

    pub async fn handle_next_timeout(&mut self) -> bool {
        if let Some((query, tid)) =
            self.outstanding_queries.next().await.map(delay_queue::Expired::into_inner)
        {
            self.tid_to_key.remove(&tid);
            _ = query.response_sink.send(Err(Error::Timeout));
            true
        } else {
            false
        }
    }
}

use crate::peers::{
    ConnectionRunner, DownloadChannel, DownloadMonitor, ListenMonitor, UploadChannel, UploadMonitor,
};
use crate::tracker::http::TrackerResponseContent;
use crate::tracker::udp::AnnounceResponse;
use futures::future::{select_all, LocalBoxFuture};
use std::{io, mem};

pub enum OperationOutput {
    DownloadFromPeer(Box<DownloadMonitor>),
    UploadToPeer(Box<UploadMonitor>),
    PeerConnectivity(Box<io::Result<(DownloadChannel, UploadChannel, ConnectionRunner)>>),
    PeerListen(Box<ListenMonitor>),
    UdpAnnounce(Box<io::Result<AnnounceResponse>>),
    HttpAnnounce(Box<io::Result<TrackerResponseContent>>),
    Void,
}

pub type Operation<'o> = LocalBoxFuture<'o, OperationOutput>;

pub trait Handler<'h> {
    fn first_operations(&mut self) -> Vec<Operation<'h>>;
    fn next_operations(&mut self, last_operation_result: OperationOutput) -> Vec<Operation<'h>>;
}

pub struct Dispatcher<'d, H: Handler<'d>> {
    handler: H,
    ops: Vec<Operation<'d>>,
}

impl<'d, H: Handler<'d>> Dispatcher<'d, H> {
    pub fn new(mut handler: H) -> Self {
        let ops = handler.first_operations();
        Self { handler, ops }
    }

    pub async fn dispatch_one(&mut self) -> bool {
        if self.ops.is_empty() {
            return false;
        }
        let current_ops = mem::take(&mut self.ops);
        let (finished_output, _finished_index, mut pending_ops) =
            select_all(current_ops.into_iter()).await;
        let mut next_ops = self.handler.next_operations(finished_output);
        self.ops.append(&mut next_ops);
        self.ops.append(&mut pending_ops);
        return true;
    }
}

use super::{listeners, Context, State};
use crate::data::{BlockAccountant, PieceInfo, PieceTracker};
use crate::pwp::{BlockInfo, DownloaderMessage, UploaderMessage};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::collections::{btree_map::Entry, BTreeMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::{Duration, Instant};
use std::{iter, mem, rc::Rc};

pub struct FakeDownloadMonitor {
    pub am_interested: Cell<bool>,
    pub peer_choking: Cell<bool>,
    pub bytes_received: Cell<usize>,
    pub requested_blocks: HashSet<BlockInfo>,
    pub submitted_msgs: RefCell<VecDeque<DownloaderMessage>>,
}

impl Default for FakeDownloadMonitor {
    fn default() -> Self {
        Self {
            am_interested: Cell::new(false),
            peer_choking: Cell::new(true),
            bytes_received: Cell::new(0),
            requested_blocks: Default::default(),
            submitted_msgs: Default::default(),
        }
    }
}

impl listeners::DownloadChannelMonitor for FakeDownloadMonitor {
    fn am_interested(&self) -> bool {
        self.am_interested.get()
    }

    fn peer_choking(&self) -> bool {
        self.peer_choking.get()
    }

    fn remote_availability(&self) -> &BlockAccountant {
        unimplemented!()
    }

    fn bytes_received(&self) -> usize {
        self.bytes_received.get()
    }

    fn requested_blocks(&self) -> Box<dyn Iterator<Item = &BlockInfo> + '_> {
        Box::new(self.requested_blocks.iter())
    }

    fn submit_outbound(&self, msg: DownloaderMessage) {
        match &msg {
            DownloaderMessage::Interested => self.am_interested.set(true),
            DownloaderMessage::NotInterested => self.am_interested.set(false),
            _ => (),
        }
        self.submitted_msgs.borrow_mut().push_back(msg);
    }

    fn pending_outbound_count(&self) -> usize {
        self.submitted_msgs.borrow().len()
    }
}

pub struct FakeUploadMonitor {
    pub peer_interested: Cell<bool>,
    pub am_choking: Cell<bool>,
    pub bytes_sent: Cell<usize>,
    pub submitted_msgs: RefCell<VecDeque<UploaderMessage>>,
}

impl Default for FakeUploadMonitor {
    fn default() -> Self {
        Self {
            peer_interested: Cell::new(false),
            am_choking: Cell::new(true),
            bytes_sent: Cell::new(0),
            submitted_msgs: Default::default(),
        }
    }
}

impl listeners::UploadChannelMonitor for FakeUploadMonitor {
    fn peer_interested(&self) -> bool {
        self.peer_interested.get()
    }

    fn am_choking(&self) -> bool {
        self.am_choking.get()
    }

    fn bytes_sent(&self) -> usize {
        self.bytes_sent.get()
    }

    fn submit_outbound(&self, msg: UploaderMessage) {
        match &msg {
            UploaderMessage::Choke => self.am_choking.set(true),
            UploaderMessage::Unchoke => self.am_choking.set(false),
            _ => (),
        }
        self.submitted_msgs.borrow_mut().push_back(msg)
    }
}

#[derive(Default)]
pub struct FakeMonitorOwner {
    pub monitors: BTreeMap<SocketAddr, (FakeDownloadMonitor, FakeUploadMonitor)>,
    next_port: u16,
}

impl FakeMonitorOwner {
    pub fn add_peer(&mut self) -> (SocketAddr, &FakeDownloadMonitor, &FakeUploadMonitor) {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.next_port));
        self.next_port += 1;
        assert!(self
            .monitors
            .insert(addr, (FakeDownloadMonitor::default(), FakeUploadMonitor::default()))
            .is_none());
        let (dm, um) = self.monitors.get(&addr).unwrap();
        (addr, dm, um)
    }
}

impl listeners::MonitorOwner for FakeMonitorOwner {
    fn download_monitor(
        &self,
        remote_ip: &SocketAddr,
    ) -> Option<&dyn listeners::DownloadChannelMonitor> {
        let (download, _upload) = self.monitors.get(remote_ip)?;
        Some(download)
    }

    fn upload_monitor(
        &self,
        remote_ip: &SocketAddr,
    ) -> Option<&dyn listeners::UploadChannelMonitor> {
        let (_download, upload) = self.monitors.get(remote_ip)?;
        Some(upload)
    }

    fn all_download_monitors(
        &self,
    ) -> Box<dyn Iterator<Item = (&SocketAddr, &dyn listeners::DownloadChannelMonitor)> + '_> {
        Box::new(self.monitors.iter().map(|(addr, (download, _upload))| {
            (addr, download as &dyn listeners::DownloadChannelMonitor)
        }))
    }

    fn all_upload_monitors(
        &self,
    ) -> Box<dyn Iterator<Item = (&SocketAddr, &dyn listeners::UploadChannelMonitor)> + '_> {
        Box::new(self.monitors.iter().map(|(addr, (_download, upload))| {
            (addr, upload as &dyn listeners::UploadChannelMonitor)
        }))
    }

    fn all_monitors(
        &self,
    ) -> Box<
        dyn Iterator<
                Item = (
                    &dyn listeners::DownloadChannelMonitor,
                    &dyn listeners::UploadChannelMonitor,
                ),
            > + '_,
    > {
        fn to_dyn_ref_pair(
            val: &(FakeDownloadMonitor, FakeUploadMonitor),
        ) -> (&dyn listeners::DownloadChannelMonitor, &dyn listeners::UploadChannelMonitor)
        {
            let (download, upload) = val;
            (download, upload)
        }
        Box::new(self.monitors.values().map(to_dyn_ref_pair))
    }
}

type DelayedOperation = Box<dyn FnOnce(&mut Context) + 'static>;

struct FakeTimer {
    pending: BTreeMap<Instant, Vec<DelayedOperation>>,
    now: Instant,
}

impl Default for FakeTimer {
    fn default() -> Self {
        Self {
            pending: Default::default(),
            now: Instant::now(),
        }
    }
}

impl listeners::Timer for FakeTimer {
    fn schedule(&mut self, delay: std::time::Duration, f: DelayedOperation) {
        let time = self.now + delay;
        let ops = self.pending.entry(time).or_default();
        ops.push(f);
    }
}

pub struct Fixture {
    pub piece_info: Rc<PieceInfo>,
    pub block_accountant: BlockAccountant,
    pub piece_tracker: PieceTracker,
    pub monitor_owner: FakeMonitorOwner,
    pub state: State,
    timer: FakeTimer,
}

impl Fixture {
    pub fn new(piece_count: usize, piece_len: usize) -> Self {
        let piece_info = Rc::new(PieceInfo::new(
            iter::repeat_with(|| [0u8; 20].as_slice()).take(piece_count),
            piece_len,
            piece_len * piece_count,
        ));

        let block_accountant = BlockAccountant::new(piece_info.clone());
        let piece_tracker = PieceTracker::new(piece_count);
        let monitor_owner = FakeMonitorOwner::default();
        let state = State::default();
        let timer = FakeTimer::default();

        Self {
            piece_info,
            block_accountant,
            piece_tracker,
            monitor_owner,
            state,
            timer,
        }
    }

    pub fn advance_time(&mut self, duration: Duration) {
        let new_now = self.timer.now + duration;
        while let Some((&time, ops)) = self.timer.pending.iter_mut().next() {
            if time > new_now {
                break;
            }
            self.timer.now = time;
            let ops = mem::take(ops);
            let mut ctx = self.ctx();
            for op in ops {
                op(&mut ctx);
            }

            if let Entry::Occupied(entry) = self.timer.pending.entry(time) {
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
        self.timer.now = new_now;
    }

    pub fn ctx(&mut self) -> Context {
        Context {
            piece_info: &self.piece_info,
            local_availability: &self.block_accountant,
            piece_tracker: &self.piece_tracker,
            monitor_owner: &self.monitor_owner,
            state: &mut self.state,
            timer: &mut self.timer,
        }
    }
}

mod tests {
    use super::listeners::TimerExt;
    use super::*;
    use crate::sec;

    #[test]
    fn test_peers_stored_in_the_order_of_creation() {
        let mut monitors = FakeMonitorOwner::default();

        let mut ips = Vec::new();
        for _ in 0..50 {
            let (ip, _dm, _um) = monitors.add_peer();
            ips.push(ip);
        }

        for (i, (ip, (_, _))) in monitors.monitors.iter().enumerate() {
            assert_eq!(&ips[i], ip);
        }
    }

    #[test]
    fn test_fake_timer_advances_time() {
        let mut f = Fixture::new(10, 1024);

        let results = Rc::new(RefCell::new(Vec::new()));

        let ctx = f.ctx();

        let results_copy = results.clone();
        ctx.timer.schedule_detached(sec!(2), move |_ctx| {
            results_copy.borrow_mut().push(1);
        });

        let results_copy = results.clone();
        ctx.timer.schedule_detached(sec!(3), move |_ctx| {
            results_copy.borrow_mut().push(2);
        });

        let results_copy = results.clone();
        ctx.timer.schedule_detached(sec!(4), move |ctx| {
            results_copy.borrow_mut().push(3);

            let results_copy_2 = results_copy.clone();
            ctx.timer.schedule_detached(sec!(0), move |_ctx| {
                results_copy_2.borrow_mut().push(4);
            });

            ctx.timer.schedule_detached(sec!(1), move |_ctx| {
                results_copy.borrow_mut().push(5);
            });
        });

        assert!(results.borrow().is_empty());

        f.advance_time(sec!(1));
        assert!(results.borrow().is_empty());

        f.advance_time(sec!(1));
        assert_eq!(vec![1], results.borrow().clone());

        f.advance_time(sec!(2));
        assert_eq!(vec![1, 2, 3, 4], results.borrow().clone());

        f.advance_time(sec!(1));
        assert_eq!(vec![1, 2, 3, 4, 5], results.borrow().clone());
    }
}

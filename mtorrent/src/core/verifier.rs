use super::{ctx, peer};
use futures_util::{Stream, StreamExt};
use local_async_utils::prelude::*;
use mtorrent_base::data;
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tokio::select;
use tokio::sync::broadcast;

pub fn piece_verifier(
    handle: ctx::Handle<ctx::MainCtx>,
    storage: data::StorageClient,
    progress_channel_capacity: usize,
) -> (VerifierHandle, Verifier) {
    let (cmd_tx, cmd_rx) = local_unbounded::channel();
    let progress_reporter = broadcast::Sender::new(progress_channel_capacity);

    (
        VerifierHandle {
            cmd_tx,
            progress_reporter: progress_reporter.clone(),
        },
        Verifier {
            cmd_rx,
            data: Data {
                handle,
                storage,
                progress_reporter,
                peer_channels: HashMap::with_capacity(peer::MainConnectionData::MAX_CONNECTIONS),
                trusted_peers: HashSet::with_capacity(peer::MainConnectionData::MAX_CONNECTIONS),
            },
        },
    )
}

#[derive(Clone)]
pub struct VerifierHandle {
    cmd_tx: local_unbounded::Sender<Command>,
    progress_reporter: broadcast::Sender<usize>,
}

impl VerifierHandle {
    pub fn register_peer(&self, peer_addr: SocketAddr) -> io::Result<local_bounded::Sender<usize>> {
        let (piece_tx, piece_rx) = local_bounded::channel(128);
        self.cmd_tx.send(Command::AddPeer {
            peer_addr,
            piece_channel: piece_rx,
        })?;
        Ok(piece_tx)
    }

    /// Subscribe to indices of pieces that have been successfully verified.
    pub fn subscribe(&self) -> broadcast::Receiver<usize> {
        self.progress_reporter.subscribe()
    }
}

pub struct Verifier {
    cmd_rx: local_unbounded::Receiver<Command>,
    data: Data,
}

impl Verifier {
    pub async fn run(self) {
        if let Err(e) = self.run_impl().await {
            log::error!("Piece verifier exited with error: {e}");
        }
    }

    async fn run_impl(self) -> Result<(), io::Error> {
        let Self {
            mut cmd_rx,
            mut data,
        } = self;

        loop {
            select! {
                biased;
                received = cmd_rx.next() => {
                    match received {
                        Some(Command::AddPeer {
                            peer_addr,
                            piece_channel,
                        }) => {
                            data.add_peer(peer_addr, piece_channel).await?;
                        }
                        None => break,
                    }
                }
                (peer_addr, received) = SelectNext(&mut data.peer_channels), if !data.peer_channels.is_empty() => {
                    match received {
                        Some(piece_index) => {
                            data.verify_piece(peer_addr, piece_index).await?;
                        }
                        None => {
                            data.remove_peer(&peer_addr);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

enum Command {
    AddPeer {
        peer_addr: SocketAddr,
        piece_channel: local_bounded::Receiver<usize>,
    },
}

struct Data {
    handle: ctx::Handle<ctx::MainCtx>,
    storage: data::StorageClient,
    progress_reporter: broadcast::Sender<usize>,
    peer_channels: HashMap<SocketAddr, local_bounded::Receiver<usize>>,
    trusted_peers: HashSet<SocketAddr>,
}

impl Data {
    async fn add_peer(
        &mut self,
        peer_addr: SocketAddr,
        piece_channel: local_bounded::Receiver<usize>,
    ) -> Result<(), data::Error> {
        define_with!(self.handle);

        if let Some(mut replaced) = self.peer_channels.remove(&peer_addr) {
            log::warn!("Peer {peer_addr} was replaced");
            let mut cx = Context::from_waker(Waker::noop());
            while let Poll::Ready(Some(piece)) = replaced.poll_next_unpin(&mut cx) {
                self.verify_piece(peer_addr, piece).await?;
            }
            self.trusted_peers.remove(&peer_addr);
        }

        self.peer_channels.insert(peer_addr, piece_channel);

        Ok(())
    }

    fn remove_peer(&mut self, peer_addr: &SocketAddr) {
        self.peer_channels.remove(peer_addr);
        self.trusted_peers.remove(peer_addr);
    }

    async fn verify_piece(
        &mut self,
        peer_addr: SocketAddr,
        piece_index: usize,
    ) -> Result<(), data::Error> {
        define_with!(self.handle);

        let (piece_len, global_offset, expected_sha1) = with!(|ctx| {
            let piece_len = ctx.pieces.piece_len(piece_index);
            let global_offset = ctx
                .pieces
                .global_offset(piece_index, 0, piece_len)
                .expect("Requested (and received!) invalid piece index");
            let expected_sha1: &[u8; 20] = ctx
                .pieces
                .hash_of_piece(piece_index)
                .expect("Requested (and received!) invalid piece index");
            (piece_len, global_offset, *expected_sha1)
        });

        let verification_success =
            self.storage.verify_block(global_offset, piece_len, expected_sha1).await?;

        with!(|ctx| ctx.pending_requests.clear_requests_of(piece_index));

        if verification_success {
            self.trusted_peers.insert(peer_addr);
            with!(|ctx| ctx.piece_tracker.forget_piece(piece_index));
            if let Err(e) = self.progress_reporter.send(piece_index) {
                log::warn!("Failed to broadcast verified piece {piece_index}: {e}");
            }
        } else {
            log::error!("Piece verification failed, peer={peer_addr} piece_index={piece_index}");
            with!(|ctx| ctx.accountant.remove_piece(piece_index));
            if !self.trusted_peers.contains(&peer_addr)
                && let Some(channel) = self.peer_channels.remove(&peer_addr)
            {
                with!(|ctx| discard_pieces(ctx, channel));
            }
        }

        Ok(())
    }
}

fn discard_pieces(ctx: &mut ctx::MainCtx, mut piece_rx: local_bounded::Receiver<usize>) {
    let mut cx = Context::from_waker(Waker::noop());

    while let Poll::Ready(Some(piece)) = piece_rx.poll_next_unpin(&mut cx) {
        ctx.accountant.remove_piece(piece);
    }
}

struct SelectNext<'a, T>(&'a mut T);

impl<'a, T, K, V> Future for SelectNext<'a, T>
where
    K: Copy,
    V: Stream + Unpin,
    for<'r> &'r mut T: IntoIterator<Item = (&'r K, &'r mut V)>,
    for<'r> <&'r mut T as IntoIterator>::IntoIter: ExactSizeIterator,
{
    type Output = (K, Option<<V as Stream>::Item>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let len = self.0.into_iter().len();
        assert_ne!(len, 0);

        let start_ind = rand::random_range(0..len);

        for (peer_addr, channel) in self.0.into_iter().skip(start_ind) {
            if let Poll::Ready(piece_ind) = channel.poll_next_unpin(cx) {
                return Poll::Ready((*peer_addr, piece_ind));
            }
        }

        for (peer_addr, channel) in self.0.into_iter().take(start_ind) {
            if let Poll::Ready(piece_ind) = channel.poll_next_unpin(cx) {
                return Poll::Ready((*peer_addr, piece_ind));
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
impl VerifierHandle {
    pub fn progress_reporter(&self) -> &broadcast::Sender<usize> {
        &self.progress_reporter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::startup;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use tokio::sync::broadcast::error::TryRecvError;
    use tokio::task;

    fn new_ctx() -> ctx::Handle<ctx::MainCtx> {
        let metainfo =
            startup::read_metainfo("../mtorrent-cli/tests/assets/example.torrent").unwrap();
        ctx::MainCtx::new(
            metainfo,
            [0u8; 20].into(),
            1234,
            12345,
            Ipv4Addr::LOCALHOST,
            Ipv6Addr::LOCALHOST,
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap()
    }

    /// Context and mock storage where verification fails only for `bad_pieces`.
    struct Fixture {
        ctx: ctx::Handle<ctx::MainCtx>,
        storage: data::StorageClient,
    }

    impl Fixture {
        fn new(bad_pieces: &[usize]) -> Self {
            let ctx = new_ctx();
            let bad_offsets: HashSet<usize> = ctx.with(|ctx| {
                bad_pieces
                    .iter()
                    .map(|&piece| {
                        ctx.pieces.global_offset(piece, 0, ctx.pieces.piece_len(piece)).unwrap()
                    })
                    .collect()
            });
            let storage = data::new_mock_storage_with_verifier(usize::MAX, move |offset, _len| {
                !bad_offsets.contains(&offset)
            });
            Self { ctx, storage }
        }

        fn mark_downloaded(&self, pieces: impl IntoIterator<Item = usize>) {
            self.ctx.with(|ctx| {
                for piece_index in pieces {
                    assert!(ctx.accountant.submit_piece(piece_index));
                }
            });
        }

        fn start_verifier(&self) -> VerifierHandle {
            let (handle, verifier) = piece_verifier(self.ctx.clone(), self.storage.clone(), 16);
            task::spawn_local(verifier.run());
            handle
        }
    }

    fn peer(port: u16) -> SocketAddr {
        SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)
    }

    #[tokio::test(flavor = "local")]
    async fn test_verified_piece_is_reported_and_no_longer_missing() {
        let fx = Fixture::new(&[]);
        fx.mark_downloaded([0]);
        fx.ctx.with(|ctx| ctx.pending_requests.add(0, &peer(6666)));

        let verifier = fx.start_verifier();
        let mut verified_pieces = verifier.subscribe();
        let mut piece_tx = verifier.register_peer(peer(6666)).unwrap();

        // downloaded piece is sent for verification
        piece_tx.try_send(0).unwrap();

        // it's broadcast, and no longer considered missing or requested
        task::yield_now().await;
        assert_eq!(verified_pieces.try_recv(), Ok(0));
        fx.ctx.with(|ctx| {
            assert!(ctx.accountant.has_piece(0));
            assert!(!ctx.piece_tracker.tracked_pieces_bitfield()[0]);
            assert!(!ctx.pending_requests.is_piece_requested(0));
        });
    }

    #[tokio::test(flavor = "local")]
    async fn test_failed_piece_from_untrusted_peer_discards_its_queue() {
        let fx = Fixture::new(&[0]);
        fx.mark_downloaded(0..3);

        let verifier = fx.start_verifier();
        let mut verified_pieces = verifier.subscribe();
        let mut piece_tx = verifier.register_peer(peer(6666)).unwrap();

        // untrusted peer sends a bad piece followed by good ones
        for piece_index in 0..3 {
            piece_tx.try_send(piece_index).unwrap();
        }
        task::yield_now().await;

        // nothing is verified, and all queued pieces are downloaded anew...
        assert_eq!(verified_pieces.try_recv(), Err(TryRecvError::Empty));
        fx.ctx.with(|ctx| {
            for piece_index in 0..3 {
                assert!(!ctx.accountant.has_piece(piece_index));
                assert!(ctx.piece_tracker.tracked_pieces_bitfield()[piece_index]);
            }
        });
        // ...and the peer's channel is closed
        assert!(piece_tx.try_send(3).is_err());
    }

    #[tokio::test(flavor = "local")]
    async fn test_failed_piece_from_trusted_peer_keeps_its_queue() {
        let fx = Fixture::new(&[1]);
        fx.mark_downloaded(0..3);

        let verifier = fx.start_verifier();
        let mut verified_pieces = verifier.subscribe();
        let mut piece_tx = verifier.register_peer(peer(6666)).unwrap();

        // peer earns trust with a good piece
        piece_tx.try_send(0).unwrap();
        task::yield_now().await;
        assert_eq!(verified_pieces.try_recv(), Ok(0));

        // then sends a bad piece followed by a good one
        piece_tx.try_send(1).unwrap();
        piece_tx.try_send(2).unwrap();

        // only the bad piece is discarded...
        task::yield_now().await;
        assert_eq!(verified_pieces.try_recv(), Ok(2));
        fx.ctx.with(|ctx| {
            assert!(ctx.accountant.has_piece(0));
            assert!(!ctx.accountant.has_piece(1));
            assert!(ctx.accountant.has_piece(2));
            assert!(ctx.piece_tracker.tracked_pieces_bitfield()[1]);
        });
        // ...and the peer's channel remains open
        piece_tx.try_send(3).unwrap();
        task::yield_now().await;
        assert_eq!(verified_pieces.try_recv(), Ok(3));
    }

    #[tokio::test(flavor = "local")]
    async fn test_replaced_untrusted_peer_with_bad_queued_piece() {
        let fx = Fixture::new(&[0]);
        fx.mark_downloaded(0..2);

        let mut data = Data {
            handle: fx.ctx.clone(),
            storage: fx.storage.clone(),
            progress_reporter: broadcast::Sender::new(16),
            peer_channels: HashMap::new(),
            trusted_peers: HashSet::new(),
        };
        let mut verified_pieces = data.progress_reporter.subscribe();
        let peer_addr = peer(6666);

        // old untrusted connection has a bad piece queued, followed by a good one
        let (mut old_tx, old_rx) = local_bounded::channel(16);
        data.add_peer(peer_addr, old_rx).await.unwrap();
        old_tx.try_send(0).unwrap();
        old_tx.try_send(1).unwrap();

        // new connection from the same address
        let (_new_tx, new_rx) = local_bounded::channel(16);
        data.add_peer(peer_addr, new_rx).await.unwrap();

        // the bad piece is discarded, but the good one is still verified...
        assert_eq!(verified_pieces.try_recv(), Ok(1));
        assert_eq!(verified_pieces.try_recv(), Err(TryRecvError::Empty));
        fx.ctx.with(|ctx| {
            assert!(!ctx.accountant.has_piece(0));
            assert!(ctx.accountant.has_piece(1));
        });
        // ...and the new connection is registered but hasn't earned trust yet
        assert!(data.peer_channels.contains_key(&peer_addr));
        assert!(!data.trusted_peers.contains(&peer_addr));
    }

    fn new_data() -> Data {
        Data {
            handle: new_ctx(),
            storage: data::new_mock_storage(usize::MAX),
            progress_reporter: broadcast::Sender::new(1024),
            peer_channels: HashMap::new(),
            trusted_peers: HashSet::new(),
        }
    }

    #[tokio::test]
    async fn test_replaced_untrusted_peer_verifies_queued_pieces_without_trusting_replacement() {
        let mut data = new_data();
        let mut verified_pieces = data.progress_reporter.subscribe();
        let peer_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 6666);

        // old connection has completed pieces queued but has not earned trust yet
        let (mut old_tx, old_rx) = local_bounded::channel(512);
        data.add_peer(peer_addr, old_rx).await.unwrap();
        data.handle.with(|ctx| {
            assert!(ctx.accountant.submit_piece(0));
            assert!(ctx.accountant.submit_piece(1));
        });
        old_tx.send(0).await.unwrap();
        old_tx.send(1).await.unwrap();
        assert!(!data.trusted_peers.contains(&peer_addr));

        // new connection from the same address
        let (_new_tx, new_rx) = local_bounded::channel(512);
        data.add_peer(peer_addr, new_rx).await.unwrap();

        // the old connection's queued pieces are verified instead of discarded...
        assert_eq!(verified_pieces.try_recv(), Ok(0));
        assert_eq!(verified_pieces.try_recv(), Ok(1));
        data.handle.with(|ctx| {
            for piece_index in 0..2 {
                assert!(ctx.accountant.has_piece(piece_index));
                assert!(!ctx.piece_tracker.tracked_pieces_bitfield()[piece_index]);
            }
        });
        // ...but the new connection has not earned trust yet
        assert!(data.peer_channels.contains_key(&peer_addr));
        assert!(!data.trusted_peers.contains(&peer_addr));
    }

    #[tokio::test]
    async fn test_replaced_good_peer_is_not_trusted_after_queued_pieces_verified() {
        let mut data = new_data();
        let mut verified_pieces = data.progress_reporter.subscribe();
        let peer_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 6666);

        // old connection with one verified piece and one more queued
        let (mut old_tx, old_rx) = local_bounded::channel(512);
        data.add_peer(peer_addr, old_rx).await.unwrap();
        data.verify_piece(peer_addr, 0).await.unwrap();
        assert!(data.trusted_peers.contains(&peer_addr));
        old_tx.send(1).await.unwrap();

        // new connection from the same address
        let (_new_tx, new_rx) = local_bounded::channel(512);
        data.add_peer(peer_addr, new_rx).await.unwrap();

        // the queued piece from the old connection has been verified...
        assert_eq!(verified_pieces.try_recv(), Ok(0));
        assert_eq!(verified_pieces.try_recv(), Ok(1));
        assert!(data.peer_channels.contains_key(&peer_addr));
        // ...but the new connection hasn't earned trust yet
        assert!(!data.trusted_peers.contains(&peer_addr));
    }
}

use crate::{data, pwp, utils::canceller};
use std::{net::SocketAddr, time::Duration};

pub trait DownloadChannelMonitor {
    fn am_interested(&self) -> bool;
    fn peer_choking(&self) -> bool;
    fn remote_availability(&self) -> &data::BlockAccountant;
    fn bytes_received(&self) -> usize;
    fn submit_outbound(&self, msg: pwp::DownloaderMessage);
}

pub trait UploadChannelMonitor {
    fn peer_interested(&self) -> bool;
    fn am_choking(&self) -> bool;
    fn bytes_sent(&self) -> usize;
    fn submit_outbound(&self, msg: pwp::UploaderMessage);
}

pub trait MonitorOwner {
    fn download_monitor(&self, remote_ip: &SocketAddr) -> Option<&dyn DownloadChannelMonitor>;
    fn upload_monitor(&self, remote_ip: &SocketAddr) -> Option<&dyn UploadChannelMonitor>;

    fn all_download_monitors(
        &self,
    ) -> Box<dyn Iterator<Item = (&SocketAddr, &dyn DownloadChannelMonitor)> + '_>;
    fn all_upload_monitors(
        &self,
    ) -> Box<dyn Iterator<Item = (&SocketAddr, &dyn UploadChannelMonitor)> + '_>;
    fn all_monitors(
        &self,
    ) -> Box<dyn Iterator<Item = (&dyn DownloadChannelMonitor, &dyn UploadChannelMonitor)> + '_>;
}

pub trait Timer {
    fn schedule(&mut self, delay: Duration, f: Box<dyn FnOnce(&mut super::Context) + 'static>);
}

pub trait TimerExt: Timer {
    fn schedule_detached<F>(&mut self, delay: Duration, f: F)
    where
        F: FnOnce(&mut super::Context) + 'static,
    {
        self.schedule(delay, Box::new(f));
    }

    fn schedule_tracked<F>(&mut self, delay: Duration, f: F) -> canceller::DetachingHandle
    where
        F: FnOnce(&mut super::Context) + 'static,
    {
        let (handle, token) = canceller::new_detaching();
        self.schedule(
            delay,
            Box::new(move |ctx| {
                if !token.is_cancellation_requested() {
                    f(ctx);
                }
            }),
        );
        handle
    }

    fn schedule_owned<F>(&mut self, delay: Duration, f: F) -> canceller::OwningHandle
    where
        F: FnOnce(&mut super::Context) + 'static,
    {
        let (handle, token) = canceller::new_owning();
        self.schedule(
            delay,
            Box::new(move |ctx| {
                if !token.is_cancellation_requested() {
                    f(ctx);
                }
            }),
        );
        handle
    }
}

impl<T: Timer + ?Sized> TimerExt for T {}

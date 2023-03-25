use crate::{data, pwp};
use std::net::SocketAddr;

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

use tokio::time::Instant;

/// Utility for measuring bitrate.
pub struct BitrateGauge {
    start_time: Instant,
    bytes: usize,
}

impl BitrateGauge {
    /// New gauge returning average bitrate since now.
    pub fn new() -> BitrateGauge {
        Self {
            start_time: Instant::now(),
            bytes: 0,
        }
    }

    /// Submit the number of bytes seen since the last update.
    pub fn update(&mut self, new_bytes: usize) -> &mut Self {
        self.bytes += new_bytes;
        self
    }

    /// Get average bitrate in bits per second since the creation of the [`BitrateGauge`].
    pub fn get_bps(&self) -> usize {
        let bits = self.bytes * 8;
        let elapsed_sec = self.start_time.elapsed().as_secs_f64();
        (bits as f64 / f64::max(f64::MIN_POSITIVE, elapsed_sec)) as usize
    }
}

impl Default for BitrateGauge {
    fn default() -> Self {
        Self::new()
    }
}

use tokio::time::Instant;

pub struct BitrateGauge {
    start_time: Instant,
    bytes: usize,
}

impl BitrateGauge {
    pub fn new() -> BitrateGauge {
        Self {
            start_time: Instant::now(),
            bytes: 0,
        }
    }

    pub fn update(&mut self, new_bytes: usize) -> &mut Self {
        self.bytes += new_bytes;
        self
    }

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

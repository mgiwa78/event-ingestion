use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::{debug, info};

use crate::config::WalConfig;
use crate::event::Event;
use crate::metrics;

pub struct WalSink {
    writer: BufWriter<File>,
    path: PathBuf,
    batches_since_fsync: u64,
    fsync_every_batches: u64,
}

impl WalSink {
    pub fn open_sharded(config: &WalConfig, shard: usize) -> Result<Self> {
        let dir = Path::new(&config.dir);
        fs::create_dir_all(dir)
            .with_context(|| format!("failed to create wal dir {}", dir.display()))?;

        let filename = format!(
            "events-w{shard}-{}.ndjson",
            Utc::now().format("%Y%m%d-%H%M%S")
        );
        let path = dir.join(filename);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open wal file {}", path.display()))?;

        info!(path = %path.display(), shard, "wal file ready");

        Ok(Self {
            writer: BufWriter::new(file),
            path,
            batches_since_fsync: 0,
            fsync_every_batches: config.fsync_every_batches.max(1),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write_batch(&mut self, batch: &[Event]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let started = Instant::now();
        let mut bytes_written = 0u64;

        for event in batch {
            let line = serde_json::to_vec(event)?;
            self.writer.write_all(&line)?;
            self.writer.write_all(b"\n")?;
            bytes_written += (line.len() + 1) as u64;
        }

        self.writer.flush()?;
        self.batches_since_fsync += 1;

        let mut fsynced = false;
        if self.batches_since_fsync >= self.fsync_every_batches {
            self.sync()?;
            fsynced = true;
            self.batches_since_fsync = 0;
        }

        let elapsed = started.elapsed().as_secs_f64();
        metrics::record_wal_batch(batch.len() as u64, bytes_written, elapsed, fsynced);

        debug!(
            events = batch.len(),
            bytes = bytes_written,
            fsynced,
            "wal batch flushed"
        );

        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer
            .get_ref()
            .sync_data()
            .context("wal fsync failed")?;
        Ok(())
    }
}

impl Drop for WalSink {
    fn drop(&mut self) {
        if let Err(err) = self.sync() {
            tracing::warn!(error = %err, "final WAL sync failed during drop");
        }
    }
}

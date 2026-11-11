use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub http: HttpConfig,
    pub tcp: TcpConfig,
    pub pipeline: PipelineConfig,
    pub wal: WalConfig,
    pub limits: LimitsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    pub bind: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TcpConfig {
    pub bind: SocketAddr,
    pub max_connections: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    pub channel_capacity: usize,
    pub worker_count: usize,
    pub enqueue_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalConfig {
    pub dir: String,
    pub batch_max_events: usize,
    pub batch_max_wait_ms: u64,
    pub fsync_every_batches: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    pub max_payload_bytes: usize,
    pub max_batch_size: usize,
    pub max_source_len: usize,
}

impl PipelineConfig {
    pub fn enqueue_timeout(&self) -> Duration {
        Duration::from_millis(self.enqueue_timeout_ms)
    }
}

impl WalConfig {
    pub fn batch_max_wait(&self) -> Duration {
        Duration::from_millis(self.batch_max_wait_ms)
    }
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config from {}", path.display()))?;
        let mut cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config from {}", path.display()))?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(v) = env::var("EVENT_INGEST_HTTP_BIND") {
            if let Ok(addr) = v.parse() {
                self.http.bind = addr;
            }
        }
        if let Ok(v) = env::var("EVENT_INGEST_TCP_BIND") {
            if let Ok(addr) = v.parse() {
                self.tcp.bind = addr;
            }
        }
        if let Ok(v) = env::var("EVENT_INGEST_CHANNEL_CAPACITY") {
            if let Ok(n) = v.parse() {
                self.pipeline.channel_capacity = n;
            }
        }
        if let Ok(v) = env::var("EVENT_INGEST_WORKER_COUNT") {
            if let Ok(n) = v.parse() {
                self.pipeline.worker_count = n;
            }
        }
        if let Ok(v) = env::var("EVENT_INGEST_WAL_DIR") {
            self.wal.dir = v;
        }
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.pipeline.channel_capacity > 0,
            "pipeline.channel_capacity must be > 0"
        );
        anyhow::ensure!(
            self.pipeline.worker_count > 0,
            "pipeline.worker_count must be > 0"
        );
        anyhow::ensure!(
            self.wal.batch_max_events > 0,
            "wal.batch_max_events must be > 0"
        );
        anyhow::ensure!(
            self.tcp.max_connections > 0,
            "tcp.max_connections must be > 0"
        );
        Ok(())
    }
}

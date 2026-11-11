use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};
use tracing::{error, info};

use crate::config::WalConfig;
use crate::event::Event;
use crate::sink::WalSink;

pub fn spawn_workers(
    receivers: Vec<mpsc::Receiver<Event>>,
    wal_config: WalConfig,
) -> Vec<JoinHandle<()>> {
    receivers
        .into_iter()
        .enumerate()
        .map(|(id, rx)| {
            let cfg = wal_config.clone();
            tokio::spawn(async move {
                if let Err(err) = run_worker(id, rx, cfg).await {
                    error!(worker = id, error = %err, "worker failed");
                } else {
                    info!(worker = id, "worker stopped");
                }
            })
        })
        .collect()
}

async fn run_worker(
    worker_id: usize,
    mut rx: mpsc::Receiver<Event>,
    config: WalConfig,
) -> anyhow::Result<()> {
    let mut sink = WalSink::open_sharded(&config, worker_id)?;
    let max_events = config.batch_max_events;
    let max_wait = config.batch_max_wait();

    info!(
        worker = worker_id,
        wal = %sink.path().display(),
        "worker online"
    );

    let mut batch: Vec<Event> = Vec::with_capacity(max_events);
    let mut batch_deadline: Option<Instant> = None;

    loop {
        if batch.is_empty() {
            match rx.recv().await {
                Some(event) => {
                    batch.push(event);
                    batch_deadline = Some(Instant::now() + max_wait);
                }
                None => break,
            }
        } else {
            let remaining = batch_deadline
                .map(|d| d.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::ZERO);

            match timeout(remaining, rx.recv()).await {
                Ok(Some(event)) => {
                    batch.push(event);
                }
                Ok(None) => {
                    flush_batch(&mut sink, &mut batch)?;
                    break;
                }
                Err(_) => {
                    flush_batch(&mut sink, &mut batch)?;
                    batch_deadline = None;
                    continue;
                }
            }

            if batch.len() >= max_events {
                flush_batch(&mut sink, &mut batch)?;
                batch_deadline = None;
            }
        }
    }

    flush_batch(&mut sink, &mut batch)?;
    sink.sync()?;
    Ok(())
}

fn flush_batch(sink: &mut WalSink, batch: &mut Vec<Event>) -> anyhow::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    sink.write_batch(batch)?;
    batch.clear();
    Ok(())
}

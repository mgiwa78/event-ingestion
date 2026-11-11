use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusHandle;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::config::Config;
use crate::error::AppError;
use crate::event::Event;
use crate::metrics;

#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    senders: Vec<mpsc::Sender<Event>>,
    next_shard: AtomicUsize,
    config: Config,
    metrics: PrometheusHandle,
}

impl AppState {
    pub fn new(
        senders: Vec<mpsc::Sender<Event>>,
        config: Config,
        metrics: PrometheusHandle,
    ) -> Self {
        Self {
            inner: Arc::new(AppStateInner {
                senders,
                next_shard: AtomicUsize::new(0),
                config,
                metrics,
            }),
        }
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn metrics_handle(&self) -> &PrometheusHandle {
        &self.inner.metrics
    }

    fn next_tx(&self) -> &mpsc::Sender<Event> {
        let idx = self.inner.next_shard.fetch_add(1, Ordering::Relaxed)
            % self.inner.senders.len();
        &self.inner.senders[idx]
    }

    pub async fn enqueue(&self, event: Event, path: &'static str) -> Result<(), AppError> {
        let dur = self.inner.config.pipeline.enqueue_timeout();
        let tx = self.next_tx().clone();
        match timeout(dur, tx.send(event)).await {
            Ok(Ok(())) => {
                metrics::record_ingested(1, path);
                Ok(())
            }
            Ok(Err(_)) => {
                metrics::record_dropped(path);
                Err(AppError::Overloaded)
            }
            Err(_) => {
                metrics::record_dropped(path);
                Err(AppError::EnqueueTimeout)
            }
        }
    }

    pub async fn enqueue_many(
        &self,
        events: Vec<Event>,
        path: &'static str,
    ) -> Result<usize, AppError> {
        let mut accepted = 0usize;
        for event in events {
            self.enqueue(event, path).await?;
            accepted += 1;
        }
        Ok(accepted)
    }
}

mod config;
mod error;
mod event;
mod ingest;
mod metrics;
mod pipeline;
mod sink;
mod state;

use std::path::PathBuf;

use anyhow::Context;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::pipeline::spawn_workers;
use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let config = Config::load(&config_path)
        .with_context(|| format!("loading config from {}", config_path.display()))?;

    info!(
        http = %config.http.bind,
        tcp = %config.tcp.bind,
        workers = config.pipeline.worker_count,
        channel_capacity = config.pipeline.channel_capacity,
        "starting up"
    );

    let metrics_handle = metrics::install_prometheus()?;

    let worker_count = config.pipeline.worker_count;
    let per_shard = (config.pipeline.channel_capacity / worker_count).max(1);
    let mut senders = Vec::with_capacity(worker_count);
    let mut receivers = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let (tx, rx) = mpsc::channel(per_shard);
        senders.push(tx);
        receivers.push(rx);
    }

    let worker_handles = spawn_workers(receivers, config.wal.clone());
    let state = AppState::new(senders, config.clone(), metrics_handle);
    let cancel = CancellationToken::new();

    let http_state = state.clone();
    let http_cancel = cancel.clone();
    let http_bind = config.http.bind;
    let http_task = tokio::spawn(async move {
        let app = ingest::http::router(http_state);
        let listener = tokio::net::TcpListener::bind(http_bind)
            .await
            .expect("bind HTTP listener");
        info!(%http_bind, "http listening");

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                http_cancel.cancelled().await;
            })
            .await
            .expect("HTTP server error");
    });

    let tcp_state = state.clone();
    let tcp_cancel = cancel.clone();
    let tcp_task = tokio::spawn(async move {
        if let Err(err) = ingest::tcp::serve(tcp_state, tcp_cancel).await {
            warn!(error = %err, "TCP server exited with error");
        }
    });

    shutdown_signal().await;
    info!("got shutdown signal, draining");
    cancel.cancel();

    let _ = http_task.await;
    let _ = tcp_task.await;

    drop(state);

    for (i, handle) in worker_handles.into_iter().enumerate() {
        match handle.await {
            Ok(()) => info!(worker = i, "worker done"),
            Err(err) => warn!(worker = i, error = %err, "worker join failed"),
        }
    }

    info!("bye");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        sigterm.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::event::parse_ndjson_line;
use crate::metrics;
use crate::state::AppState;

pub async fn serve(state: AppState, cancel: CancellationToken) -> anyhow::Result<()> {
    let bind = state.config().tcp.bind;
    let max_conn = state.config().tcp.max_connections;
    let listener = TcpListener::bind(bind).await?;
    let limiter = Arc::new(Semaphore::new(max_conn));

    info!(%bind, max_conn, "tcp listening");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("tcp shutting down");
                break;
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let permit = match limiter.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!(%peer, "too many tcp connections, dropping");
                        metrics::record_dropped("tcp");
                        continue;
                    }
                };

                let state = state.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    handle_connection(stream, peer, state, permit, cancel).await;
                });
            }
        }
    }

    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    peer: std::net::SocketAddr,
    state: AppState,
    _permit: OwnedSemaphorePermit,
    cancel: CancellationToken,
) {
    metrics::record_tcp_opened();
    debug!(%peer, "TCP connection opened");

    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match parse_ndjson_line(&line, &state.config().limits) {
                            Ok(event) => {
                                if let Err(err) = state.enqueue(event, "tcp").await {
                                    warn!(%peer, error = %err, "TCP enqueue failed");
                                    break;
                                }
                            }
                            Err(err) => {
                                metrics::record_rejected("tcp");
                                warn!(%peer, error = %err, "invalid TCP event");
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!(%peer, error = %err, "TCP read error");
                        break;
                    }
                }
            }
        }
    }

    metrics::record_tcp_closed();
    debug!(%peer, "TCP connection closed");
}

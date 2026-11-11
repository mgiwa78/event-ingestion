use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::json;

use crate::error::AppError;
use crate::event::{into_event, BatchIngestRequest, IngestRequest};
use crate::metrics;
use crate::state::AppState;

#[derive(Serialize)]
struct IngestResponse {
    accepted: usize,
    ids: Vec<uuid::Uuid>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(prometheus_metrics))
        .route("/v1/events", post(ingest_one))
        .route("/v1/events/batch", post(ingest_batch))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

async fn prometheus_metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        state.metrics_handle().render(),
    )
}

async fn ingest_one(
    State(state): State<AppState>,
    Json(req): Json<IngestRequest>,
) -> Result<impl IntoResponse, AppError> {
    metrics::record_http_request("single");

    let event = match into_event(req, &state.config().limits) {
        Ok(e) => e,
        Err(err) => {
            metrics::record_rejected("http");
            return Err(err);
        }
    };
    let id = event.id;
    state.enqueue(event, "http").await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(IngestResponse {
            accepted: 1,
            ids: vec![id],
        }),
    ))
}

async fn ingest_batch(
    State(state): State<AppState>,
    Json(batch): Json<BatchIngestRequest>,
) -> Result<impl IntoResponse, AppError> {
    metrics::record_http_request("batch");

    let max = state.config().limits.max_batch_size;
    if batch.events.len() > max {
        metrics::record_rejected("http");
        return Err(AppError::BatchTooLarge {
            size: batch.events.len(),
            max,
        });
    }

    let limits = &state.config().limits;
    let mut events = Vec::with_capacity(batch.events.len());
    let mut ids = Vec::with_capacity(batch.events.len());

    for req in batch.events {
        match into_event(req, limits) {
            Ok(event) => {
                ids.push(event.id);
                events.push(event);
            }
            Err(err) => {
                metrics::record_rejected("http");
                return Err(err);
            }
        }
    }

    let accepted = state.enqueue_many(events, "http").await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(IngestResponse { accepted, ids }),
    ))
}

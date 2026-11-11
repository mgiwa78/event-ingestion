use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("invalid event: {0}")]
    InvalidEvent(String),

    #[error("payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("batch too large: {size} events (max {max})")]
    BatchTooLarge { size: usize, max: usize },

    #[error("too busy, try again")]
    Overloaded,

    #[error("queue wait timed out")]
    EnqueueTimeout,

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            AppError::InvalidEvent(_)
            | AppError::PayloadTooLarge { .. }
            | AppError::BatchTooLarge { .. } => StatusCode::BAD_REQUEST,
            AppError::Overloaded | AppError::EnqueueTimeout => StatusCode::SERVICE_UNAVAILABLE,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = Json(json!({
            "error": self.to_string(),
        }));

        (status, body).into_response()
    }
}

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::config::LimitsConfig;
use crate::error::AppError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub ts: DateTime<Utc>,
    pub source: String,
    #[serde(with = "serde_bytes_compat")]
    pub payload: Bytes,
}

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub id: Option<Uuid>,
    pub ts: Option<DateTime<Utc>>,
    pub source: String,
    pub payload: Value,
}

#[derive(Debug, Deserialize)]
pub struct BatchIngestRequest {
    pub events: Vec<IngestRequest>,
}

pub fn validate_request(req: &IngestRequest, limits: &LimitsConfig) -> Result<(), AppError> {
    validate_source(&req.source, limits.max_source_len)?;
    let size = payload_size(&req.payload);
    if size > limits.max_payload_bytes {
        return Err(AppError::PayloadTooLarge {
            size,
            max: limits.max_payload_bytes,
        });
    }
    Ok(())
}

pub fn validate_source(source: &str, max_len: usize) -> Result<(), AppError> {
    if source.is_empty() {
        return Err(AppError::InvalidEvent("source must not be empty".into()));
    }
    if source.len() > max_len {
        return Err(AppError::InvalidEvent(format!(
            "source length {} exceeds max {}",
            source.len(),
            max_len
        )));
    }
    if source.chars().any(|c| c.is_control()) {
        return Err(AppError::InvalidEvent(
            "source must not contain control characters".into(),
        ));
    }
    Ok(())
}

pub fn into_event(req: IngestRequest, limits: &LimitsConfig) -> Result<Event, AppError> {
    validate_request(&req, limits)?;
    let payload = value_to_bytes(&req.payload)?;
    Ok(Event {
        id: req.id.unwrap_or_else(Uuid::new_v4),
        ts: req.ts.unwrap_or_else(Utc::now),
        source: req.source,
        payload,
    })
}

pub fn parse_ndjson_line(line: &str, limits: &LimitsConfig) -> Result<Event, AppError> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidEvent("empty line".into()));
    }
    let req: IngestRequest = serde_json::from_str(trimmed)
        .map_err(|e| AppError::InvalidEvent(format!("invalid json: {e}")))?;
    into_event(req, limits)
}

fn payload_size(value: &Value) -> usize {
    // Approximate size; avoids a full serialize just for the limit check.
    match value {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(n) => n.to_string().len(),
        Value::String(s) => s.len(),
        Value::Array(arr) => arr.iter().map(payload_size).sum::<usize>() + arr.len(),
        Value::Object(map) => {
            map.iter()
                .map(|(k, v)| k.len() + payload_size(v))
                .sum::<usize>()
                + map.len()
        }
    }
}

fn value_to_bytes(value: &Value) -> Result<Bytes, AppError> {
    let vec = serde_json::to_vec(value)
        .map_err(|e| AppError::InvalidEvent(format!("payload serialize failed: {e}")))?;
    Ok(Bytes::from(vec))
}

mod serde_bytes_compat {
    use bytes::Bytes;
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_json::Value;

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value: Value = serde_json::from_slice(bytes).unwrap_or(Value::Null);
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let vec = serde_json::to_vec(&value).map_err(D::Error::custom)?;
        Ok(Bytes::from(vec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LimitsConfig;

    fn limits() -> LimitsConfig {
        LimitsConfig {
            max_payload_bytes: 1024,
            max_batch_size: 10,
            max_source_len: 32,
        }
    }

    #[test]
    fn rejects_empty_source() {
        let req = IngestRequest {
            id: None,
            ts: None,
            source: String::new(),
            payload: Value::Null,
        };
        assert!(validate_request(&req, &limits()).is_err());
    }

    #[test]
    fn into_event_owns_payload() {
        let req = IngestRequest {
            id: None,
            ts: None,
            source: "web".into(),
            payload: serde_json::json!({"a": 1}),
        };
        let event = into_event(req, &limits()).unwrap();
        assert_eq!(event.source, "web");
        assert!(!event.payload.is_empty());
    }
}

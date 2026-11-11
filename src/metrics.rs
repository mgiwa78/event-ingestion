use metrics::{counter, describe_counter, describe_histogram, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub fn install_prometheus() -> anyhow::Result<PrometheusHandle> {
    let handle = PrometheusBuilder::new().install_recorder()?;

    describe_counter!(
        "events_ingested_total",
        "Events accepted into the ingestion pipeline"
    );
    describe_counter!(
        "events_rejected_total",
        "Events rejected during validation or parse"
    );
    describe_counter!(
        "events_dropped_total",
        "Events dropped due to backpressure / timeout"
    );
    describe_counter!("wal_events_written_total", "Events appended to the WAL");
    describe_counter!("wal_bytes_written_total", "Bytes appended to the WAL");
    describe_counter!("wal_batches_total", "WAL flush batches completed");
    describe_counter!("wal_fsync_total", "WAL fsync operations");
    describe_histogram!("wal_batch_size", "Number of events per WAL batch");
    describe_histogram!(
        "wal_flush_duration_seconds",
        "Time spent flushing a WAL batch"
    );
    describe_counter!("tcp_connections_opened_total", "TCP connections accepted");
    describe_counter!("tcp_connections_closed_total", "TCP connections closed");
    describe_counter!("http_requests_total", "HTTP ingest requests");

    Ok(handle)
}

pub fn record_ingested(n: u64, path: &'static str) {
    counter!("events_ingested_total", "path" => path).increment(n);
}

pub fn record_rejected(path: &'static str) {
    counter!("events_rejected_total", "path" => path).increment(1);
}

pub fn record_dropped(path: &'static str) {
    counter!("events_dropped_total", "path" => path).increment(1);
}

pub fn record_http_request(route: &'static str) {
    counter!("http_requests_total", "route" => route).increment(1);
}

pub fn record_tcp_opened() {
    counter!("tcp_connections_opened_total").increment(1);
}

pub fn record_tcp_closed() {
    counter!("tcp_connections_closed_total").increment(1);
}

pub fn record_wal_batch(events: u64, bytes: u64, duration_secs: f64, fsynced: bool) {
    counter!("wal_events_written_total").increment(events);
    counter!("wal_bytes_written_total").increment(bytes);
    counter!("wal_batches_total").increment(1);
    histogram!("wal_batch_size").record(events as f64);
    histogram!("wal_flush_duration_seconds").record(duration_secs);
    if fsynced {
        counter!("wal_fsync_total").increment(1);
    }
}

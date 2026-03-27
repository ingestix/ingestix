use ingestix::{
    ApiKeyConfig, FlowWorker, HttpConfig, HttpIngestor, HttpQueuePolicy, Ingestix, SharedContext,
};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

#[derive(FlowWorker)]
#[flow(message = "Value", config = "()", state = "()")]
struct LoggingWorker;

impl LoggingWorker {
    async fn handle(&self, msg: Value, _ctx: Arc<SharedContext<(), ()>>) -> anyhow::Result<()> {
        // Business logic for unknown schema: inspect/use raw JSON value.
        // Avoid logging the full payload by default (it may contain sensitive data).
        let kind = if msg.is_object() {
            "object"
        } else if msg.is_array() {
            "array"
        } else if msg.is_string() {
            "string"
        } else {
            "other"
        };

        let size = if msg.is_object() {
            msg.as_object().map(|o| o.len()).unwrap_or(0)
        } else if msg.is_array() {
            msg.as_array().map(|a| a.len()).unwrap_or(0)
        } else {
            0
        };

        tracing::info!(
            "Worker processed generic JSON payload (kind={}, size={})",
            kind,
            size
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable structured logs for both the ingestor and worker.
    tracing_subscriber::fmt::init();

    // Ingestix: concurrency=10 workers, channel buffer=100 messages.
    let runner = Ingestix::new((), (), 10, 100);

    // Expose metrics and health endpoints.
    runner.spawn_monitor_server(8080).await?;
    tracing::info!("Monitor server running at http://localhost:8080");

    // HTTP endpoint with request guards:
    // - `require_json: true` rejects non-JSON Content-Type with 415.
    // - HTTP auth is enabled when `INGESTIX_API_KEY` is set.
    let ingestor = HttpIngestor::with_config(
        "0.0.0.0:3000".parse()?,
        "/ingest",
        HttpConfig {
            api_key: api_key_from_env(),
            max_body_bytes: 2 * 1024 * 1024,
            request_timeout: Duration::from_secs(15),
            enqueue_timeout: Duration::from_secs(3),
            queue_policy: HttpQueuePolicy::Reject429,
            ..HttpConfig::default()
        },
    );

    if ingestor.api_key.is_none() {
        tracing::warn!("INGESTIX_API_KEY not set; HTTP auth is disabled for this example");
    }
    tracing::info!("Advanced webhook ingestor listening at http://localhost:3000/ingest");

    // Start ingestion + worker processing loop.
    runner.launch(ingestor, LoggingWorker).await
}

fn api_key_from_env() -> Option<ApiKeyConfig> {
    let Ok(value) = std::env::var("INGESTIX_API_KEY") else {
        return None;
    };

    let value = value.trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(ApiKeyConfig::x_api_key(value))
    }
}

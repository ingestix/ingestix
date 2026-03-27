use ingestix::{FlowWorker, HttpIngestor, Ingestix, SharedContext};
use serde_json::Value;
use std::sync::Arc;

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

    // HTTP endpoint that ingests generic JSON bodies.
    let ingestor = HttpIngestor::new("0.0.0.0:3000".parse()?, "/ingest");
    tracing::info!("Webhook Ingestor listening at http://localhost:3000/ingest");

    // Start ingestion + worker processing loop.
    runner.launch(ingestor, LoggingWorker).await
}

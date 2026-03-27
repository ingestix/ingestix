use ingestix::{FlowWorker, HttpIngestor, Ingestix, SharedContext};
use std::sync::Arc;

// Typed payload expected in the HTTP POST body.
#[derive(serde::Deserialize, serde::Serialize, Debug)]
struct WebhookPayload {
    event: String,
    data: String,
}

#[derive(FlowWorker)]
#[flow(message = "WebhookPayload", config = "()", state = "()")]
struct LoggingWorker;

impl LoggingWorker {
    async fn handle(
        &self,
        msg: WebhookPayload,
        _ctx: Arc<SharedContext<(), ()>>,
    ) -> anyhow::Result<()> {
        // Business logic: process the typed payload.
        tracing::info!(
            "Worker processed message: event='{}', data_len={}",
            msg.event,
            msg.data.len()
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

    // HTTP endpoint that ingests typed JSON payloads.
    let ingestor = HttpIngestor::new("0.0.0.0:3000".parse()?, "/ingest");
    tracing::info!("Webhook Ingestor listening at http://localhost:3000/ingest");

    // Start ingestion + worker processing loop.
    runner.launch(ingestor, LoggingWorker).await
}

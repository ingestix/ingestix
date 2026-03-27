use ingestix::{FlowWorker, HttpIngestor, Ingestix, SharedContext};
use std::sync::Arc;

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
        tracing::info!(
            "Minimal worker processed message: event='{}', data_len={}",
            msg.event,
            msg.data.len()
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Minimal setup: ingestion + worker only (no monitor server / metrics feature).
    let runner = Ingestix::new((), (), 10, 100);

    let ingestor = HttpIngestor::new("0.0.0.0:3000".parse()?, "/ingest");
    tracing::info!("Minimal webhook ingestor listening at http://localhost:3000/ingest");

    runner.launch(ingestor, LoggingWorker).await
}

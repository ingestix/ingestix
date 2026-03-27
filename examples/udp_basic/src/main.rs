use ingestix::{FlowWorker, Ingestix, SharedContext, UdpConfig, UdpIngestor};
use std::sync::Arc;

// Typed UDP message expected as JSON.
#[derive(serde::Deserialize, Debug)]
struct MyMsg {
    id: String,
    val: f64,
}

#[derive(FlowWorker)]
#[flow(message = "MyMsg", config = "()", state = "()")]
struct MyWorker;

impl MyWorker {
    async fn handle(&self, msg: MyMsg, _ctx: Arc<SharedContext<(), ()>>) -> anyhow::Result<()> {
        // Business logic for each incoming UDP payload.
        println!("Received: id={} val={}", msg.id, msg.val);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable logs and initialize the runtime pipeline.
    tracing_subscriber::fmt::init();

    // Ingestix: concurrency=10 workers, channel buffer=100 messages.
    let runner = Ingestix::new((), (), 10, 100);

    // Expose metrics and health endpoints.
    runner.spawn_monitor_server(8080).await?;

    // UDP socket that accepts JSON datagrams.
    let addr = "0.0.0.0:9000".parse()?;

    let ingestor = UdpIngestor::with_config(
        addr,
        UdpConfig {
            max_datagram_size: 8 * 1024,
            recv_buffer_size: 64 * 1024,
        },
    );

    // Start ingestion + worker processing loop.
    runner.launch(ingestor, MyWorker).await
}

use ingestix::{FlowWorker, Ingestix, SharedContext, SyslogEvent, SyslogIngestor};
use std::sync::Arc;

#[derive(FlowWorker)]
#[flow(message = "SyslogEvent", config = "()", state = "()")]
struct SyslogLogger;

impl SyslogLogger {
    async fn handle(
        &self,
        msg: SyslogEvent,
        _ctx: Arc<SharedContext<(), ()>>,
    ) -> anyhow::Result<()> {
        println!(
            "Syslog received: protocol={:?} host={:?} message={}",
            msg.protocol, msg.hostname, msg.message
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Ingestix: concurrency=10 workers, channel buffer=100 messages.
    let runner = Ingestix::new((), (), 10, 100);
    runner.spawn_monitor_server(8080).await?;

    // Bind a UDP socket for syslog lines.
    let addr = "0.0.0.0:9000".parse()?;
    let ingestor = SyslogIngestor::new(addr);
    println!("Syslog ingestor listening on udp://{addr}");

    runner.launch(ingestor, SyslogLogger).await
}

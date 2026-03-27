#![cfg(feature = "ingestors")]

use ingestix::{Ingestix, UdpIngestor, Worker};
use serial_test::serial;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep, timeout};

#[derive(serde::Deserialize)]
struct UdpMsg {
    #[allow(dead_code)]
    value: u64,
}

struct SlowNoopWorker;

#[ingestix::async_trait]
impl Worker<UdpMsg, (), ()> for SlowNoopWorker {
    async fn process(
        &self,
        _msg: UdpMsg,
        _ctx: Arc<ingestix::SharedContext<(), ()>>,
    ) -> anyhow::Result<()> {
        sleep(Duration::from_millis(300)).await;
        Ok(())
    }
}

fn reserve_local_addr() -> std::net::SocketAddr {
    let listener = std::net::UdpSocket::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn launch_exits_with_udp_backpressure_after_sigterm() {
    let addr = reserve_local_addr();
    let ingestor = UdpIngestor::new(addr);
    let runner = Ingestix::<UdpMsg, (), ()>::new((), (), 1, 1);

    let launch_task = tokio::spawn(async move { runner.launch(ingestor, SlowNoopWorker).await });
    sleep(Duration::from_millis(120)).await;

    let sender = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind sender UDP socket");
    for i in 0..4_u64 {
        let payload = format!("{{\"value\":{i}}}");
        sender
            .send_to(payload.as_bytes(), addr)
            .await
            .expect("send UDP payload");
    }

    // Allow signal listener setup to complete before sending SIGTERM.
    sleep(Duration::from_millis(150)).await;
    let signal_result = unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
    assert_eq!(signal_result, 0, "failed to send SIGTERM");

    let launch_result = timeout(Duration::from_secs(4), launch_task)
        .await
        .expect("launch should complete under UDP backpressure on shutdown")
        .expect("launch task should not panic");
    assert!(
        launch_result.is_ok(),
        "launch returned error after SIGTERM: {launch_result:?}"
    );
}

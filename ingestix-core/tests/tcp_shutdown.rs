#![cfg(feature = "ingestors")]

use ingestix::{Ingestix, TcpIngestor, Worker};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep, timeout};

struct NoopWorker;

#[ingestix::async_trait]
impl Worker<(), (), ()> for NoopWorker {
    async fn process(
        &self,
        _msg: (),
        _ctx: Arc<ingestix::SharedContext<(), ()>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

fn reserve_local_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

async fn wait_until_listening(addr: std::net::SocketAddr) {
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("TCP ingestor did not start listening in time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn launch_exits_with_idle_tcp_connection_after_sigterm() {
    let addr = reserve_local_addr();
    let ingestor = TcpIngestor::new(addr);
    let runner = Ingestix::<(), (), ()>::new((), (), 2, 16);

    let launch_task = tokio::spawn(async move { runner.launch(ingestor, NoopWorker).await });
    wait_until_listening(addr).await;

    // Keep an idle connection open; shutdown should still complete.
    let _idle_conn = TcpStream::connect(addr)
        .await
        .expect("connect idle client to TCP ingestor");

    sleep(Duration::from_millis(100)).await;
    let signal_result = unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
    assert_eq!(signal_result, 0, "failed to send SIGTERM");

    let launch_result = timeout(Duration::from_secs(3), launch_task)
        .await
        .expect("launch should complete even with idle TCP connection")
        .expect("launch task should not panic");
    assert!(
        launch_result.is_ok(),
        "launch returned error after SIGTERM: {launch_result:?}"
    );
}

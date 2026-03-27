#![cfg(feature = "ingestors")]

use ingestix::{Ingestix, SharedContext, UdpIngestor, Worker};
use serial_test::serial;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep, timeout};

#[derive(serde::Deserialize)]
struct TestMsg {
    #[allow(dead_code)]
    id: usize,
}

struct CountingWorker {
    processed: Arc<AtomicUsize>,
}

#[ingestix::async_trait]
impl Worker<TestMsg, (), ()> for CountingWorker {
    async fn process(&self, _msg: TestMsg, _ctx: Arc<SharedContext<(), ()>>) -> anyhow::Result<()> {
        self.processed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn reserve_local_addr() -> std::net::SocketAddr {
    let socket = std::net::UdpSocket::bind(("127.0.0.1", 0)).expect("bind ephemeral UDP port");
    let addr = socket.local_addr().expect("read local addr");
    drop(socket);
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn udp_ingestor_accepts_datagrams_processes_them_and_shuts_down() {
    let addr = reserve_local_addr();
    let ingestor = UdpIngestor::new(addr);
    let processed = Arc::new(AtomicUsize::new(0));
    let worker = CountingWorker {
        processed: processed.clone(),
    };
    let target_count = 10;

    let runner = Ingestix::<TestMsg, (), ()>::new((), (), 2, 32);
    let launch_task = tokio::spawn(async move { runner.launch(ingestor, worker).await });

    // Give ingestor a brief moment to bind the socket.
    sleep(Duration::from_millis(80)).await;

    let sender = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind UDP sender socket");
    for i in 0..target_count {
        let payload = format!("{{\"id\":{i}}}");
        sender
            .send_to(payload.as_bytes(), addr)
            .await
            .expect("send UDP datagram");
    }

    timeout(Duration::from_secs(3), async {
        loop {
            if processed.load(Ordering::SeqCst) >= target_count {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker should process all accepted UDP datagrams");
    assert_eq!(
        processed.load(Ordering::SeqCst),
        target_count,
        "all accepted UDP datagrams should be processed"
    );

    let signal_result = unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
    assert_eq!(signal_result, 0, "failed to send SIGTERM");

    let launch_result = timeout(Duration::from_secs(4), launch_task)
        .await
        .expect("launch should complete after shutdown signal")
        .expect("launch task should not panic");
    assert!(
        launch_result.is_ok(),
        "launch returned error: {launch_result:?}"
    );
}

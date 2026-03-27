#![cfg(feature = "ingestors")]

use ingestix::{Ingestix, SharedContext, TcpIngestor, Worker};
use serial_test::serial;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
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

async fn send_len_delimited_json(stream: &mut TcpStream, payload: &str) {
    let len = payload.len() as u32;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(payload.as_bytes());
    stream
        .write_all(&frame)
        .await
        .expect("write TCP frame to ingestor");
    stream.flush().await.expect("flush TCP frame");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn tcp_ingestor_accepts_frames_processes_them_and_shuts_down() {
    let addr = reserve_local_addr();
    let ingestor = TcpIngestor::new(addr);
    let processed = Arc::new(AtomicUsize::new(0));
    let worker = CountingWorker {
        processed: processed.clone(),
    };
    let target_count = 10;

    let runner = Ingestix::<TestMsg, (), ()>::new((), (), 2, 32);
    let launch_task = tokio::spawn(async move { runner.launch(ingestor, worker).await });

    wait_until_listening(addr).await;
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect test client to TCP ingestor");

    for i in 0..target_count {
        let payload = format!("{{\"id\":{i}}}");
        send_len_delimited_json(&mut stream, &payload).await;
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
    .expect("worker should process all accepted TCP frames");
    assert_eq!(
        processed.load(Ordering::SeqCst),
        target_count,
        "all accepted TCP frames should be processed"
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

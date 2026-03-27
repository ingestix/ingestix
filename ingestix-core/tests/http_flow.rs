#![cfg(feature = "ingestors")]

use ingestix::{HttpIngestor, Ingestix, SharedContext, Worker};
use serial_test::serial;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    panic!("HTTP ingestor did not start listening in time");
}

async fn send_http_json_post(addr: std::net::SocketAddr, path: &str, body: &str) -> u16 {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to HTTP ingestor");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write HTTP request");
    stream.flush().await.expect("flush HTTP request");

    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read HTTP response");
    let text = String::from_utf8(raw).expect("response should be utf8");
    let status_line = text.lines().next().expect("missing status line");
    let code = status_line
        .split_whitespace()
        .nth(1)
        .expect("missing status code")
        .parse::<u16>()
        .expect("invalid status code");
    code
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn http_ingestor_accepts_posts_processes_them_and_shuts_down() {
    let addr = reserve_local_addr();
    let path = "/ingest";
    let target_count = 10;

    let ingestor = HttpIngestor::new(addr, path);
    let processed = Arc::new(AtomicUsize::new(0));
    let worker = CountingWorker {
        processed: processed.clone(),
    };

    let runner = Ingestix::<TestMsg, (), ()>::new((), (), 2, 32);
    let launch_task = tokio::spawn(async move { runner.launch(ingestor, worker).await });

    wait_until_listening(addr).await;

    for i in 0..target_count {
        let body = format!("{{\"id\":{i}}}");
        let status = send_http_json_post(addr, path, &body).await;
        assert_eq!(status, 202, "request {i} should be accepted");
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
    .expect("worker should process all accepted messages");
    assert_eq!(
        processed.load(Ordering::SeqCst),
        target_count,
        "all accepted messages should be processed"
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

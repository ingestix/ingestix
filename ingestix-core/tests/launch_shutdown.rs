use anyhow::anyhow;
use ingestix::{Ingestix, Ingestor, SharedContext, Worker, WorkerFailurePolicy};
use serial_test::serial;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Duration, sleep, timeout};
#[cfg(feature = "metrics")]
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

struct NoopWorker;

#[ingestix::async_trait]
impl Worker<(), (), ()> for NoopWorker {
    async fn process(&self, _msg: (), _ctx: Arc<SharedContext<(), ()>>) -> anyhow::Result<()> {
        Ok(())
    }
}

struct ShutdownAwareIngestor {
    saw_shutdown: Arc<AtomicBool>,
}

#[ingestix::async_trait]
impl Ingestor<(), (), ()> for ShutdownAwareIngestor {
    async fn run(
        self,
        _tx: mpsc::Sender<()>,
        _ctx: Arc<SharedContext<(), ()>>,
        mut shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let _ = shutdown.recv().await;
        self.saw_shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct ErrorIngestor;

#[ingestix::async_trait]
impl Ingestor<(), (), ()> for ErrorIngestor {
    async fn run(
        self,
        _tx: mpsc::Sender<()>,
        _ctx: Arc<SharedContext<(), ()>>,
        _shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        Err(anyhow!("intentional ingestor failure"))
    }
}

struct SingleMessageIngestor;

#[ingestix::async_trait]
impl Ingestor<(), (), ()> for SingleMessageIngestor {
    async fn run(
        self,
        tx: mpsc::Sender<()>,
        _ctx: Arc<SharedContext<(), ()>>,
        _shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        tx.send(()).await.map_err(|e| anyhow!(e.to_string()))?;
        Ok(())
    }
}

struct SlowWorker {
    processed: Arc<AtomicBool>,
}

#[ingestix::async_trait]
impl Worker<(), (), ()> for SlowWorker {
    async fn process(&self, _msg: (), _ctx: Arc<SharedContext<(), ()>>) -> anyhow::Result<()> {
        sleep(Duration::from_millis(200)).await;
        self.processed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct BurstIngestor {
    count: usize,
}

#[ingestix::async_trait]
impl Ingestor<usize, (), ()> for BurstIngestor {
    async fn run(
        self,
        tx: mpsc::Sender<usize>,
        _ctx: Arc<SharedContext<(), ()>>,
        _shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        for i in 0..self.count {
            tx.send(i).await.map_err(|e| anyhow!(e.to_string()))?;
        }
        Ok(())
    }
}

struct ConcurrencyTrackingWorker {
    current: Arc<AtomicUsize>,
    max_seen: Arc<AtomicUsize>,
}

#[ingestix::async_trait]
impl Worker<usize, (), ()> for ConcurrencyTrackingWorker {
    async fn process(&self, _msg: usize, _ctx: Arc<SharedContext<(), ()>>) -> anyhow::Result<()> {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_seen.fetch_max(now, Ordering::SeqCst);
        sleep(Duration::from_millis(50)).await;
        self.current.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

struct BufferedThenWaitIngestor {
    count: usize,
    sent_all: Arc<AtomicBool>,
}

#[ingestix::async_trait]
impl Ingestor<usize, (), ()> for BufferedThenWaitIngestor {
    async fn run(
        self,
        tx: mpsc::Sender<usize>,
        _ctx: Arc<SharedContext<(), ()>>,
        mut shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        for i in 0..self.count {
            tx.send(i).await.map_err(|e| anyhow!(e.to_string()))?;
        }
        self.sent_all.store(true, Ordering::SeqCst);
        let _ = shutdown.recv().await;
        Ok(())
    }
}

struct CountingSlowWorker {
    processed: Arc<AtomicUsize>,
}

#[ingestix::async_trait]
impl Worker<usize, (), ()> for CountingSlowWorker {
    async fn process(&self, _msg: usize, _ctx: Arc<SharedContext<(), ()>>) -> anyhow::Result<()> {
        sleep(Duration::from_millis(40)).await;
        self.processed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct FixedBatchIngestor {
    values: Vec<usize>,
}

#[ingestix::async_trait]
impl Ingestor<usize, (), ()> for FixedBatchIngestor {
    async fn run(
        self,
        tx: mpsc::Sender<usize>,
        _ctx: Arc<SharedContext<(), ()>>,
        _shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        for value in self.values {
            tx.send(value).await.map_err(|e| anyhow!(e.to_string()))?;
        }
        Ok(())
    }
}

struct SelectiveFailWorker {
    ok_count: Arc<AtomicUsize>,
    fail_on: usize,
}

struct PanicWorker;

#[ingestix::async_trait]
impl Worker<usize, (), ()> for PanicWorker {
    async fn process(&self, _msg: usize, _ctx: Arc<SharedContext<(), ()>>) -> anyhow::Result<()> {
        panic!("intentional worker panic");
    }
}

#[ingestix::async_trait]
impl Worker<usize, (), ()> for SelectiveFailWorker {
    async fn process(&self, msg: usize, _ctx: Arc<SharedContext<(), ()>>) -> anyhow::Result<()> {
        if msg == self.fail_on {
            return Err(anyhow!("intentional worker failure for msg {msg}"));
        }
        self.ok_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn launch_broadcasts_shutdown_on_sigterm() {
    let saw_shutdown = Arc::new(AtomicBool::new(false));
    let ingestor = ShutdownAwareIngestor {
        saw_shutdown: saw_shutdown.clone(),
    };

    let runner = Ingestix::<(), (), ()>::new((), (), 1, 8);
    let launch_task = tokio::spawn(async move { runner.launch(ingestor, NoopWorker).await });

    // Give the runtime a brief moment to install signal handling and start tasks.
    sleep(Duration::from_millis(100)).await;
    let signal_result = unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
    assert_eq!(
        signal_result, 0,
        "failed to send SIGTERM to current process"
    );

    let launch_result = timeout(Duration::from_secs(3), launch_task)
        .await
        .expect("launch should complete after SIGTERM")
        .expect("launch task should not panic");
    assert!(
        launch_result.is_ok(),
        "launch returned error after SIGTERM: {launch_result:?}"
    );
    assert!(
        saw_shutdown.load(Ordering::SeqCst),
        "ingestor did not observe shutdown broadcast"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn launch_broadcasts_shutdown_on_sigint() {
    let saw_shutdown = Arc::new(AtomicBool::new(false));
    let ingestor = ShutdownAwareIngestor {
        saw_shutdown: saw_shutdown.clone(),
    };

    let runner = Ingestix::<(), (), ()>::new((), (), 1, 8);
    let launch_task = tokio::spawn(async move { runner.launch(ingestor, NoopWorker).await });

    // Give the runtime a brief moment to install signal handling and start tasks.
    sleep(Duration::from_millis(100)).await;
    let signal_result = unsafe { libc::kill(libc::getpid(), libc::SIGINT) };
    assert_eq!(signal_result, 0, "failed to send SIGINT to current process");

    let launch_result = timeout(Duration::from_secs(3), launch_task)
        .await
        .expect("launch should complete after SIGINT")
        .expect("launch task should not panic");
    assert!(
        launch_result.is_ok(),
        "launch returned error after SIGINT: {launch_result:?}"
    );
    assert!(
        saw_shutdown.load(Ordering::SeqCst),
        "ingestor did not observe shutdown broadcast"
    );
}

#[tokio::test]
async fn launch_propagates_ingestor_errors() {
    let runner = Ingestix::<(), (), ()>::new((), (), 1, 8);
    let result = runner.launch(ErrorIngestor, NoopWorker).await;

    assert!(result.is_err(), "expected launch to return ingestor error");
    let error_text = result.expect_err("launch should fail").to_string();
    assert!(
        error_text.contains("intentional ingestor failure"),
        "unexpected error text: {error_text}"
    );
}

#[tokio::test]
async fn launch_waits_for_inflight_workers_before_returning() {
    let processed = Arc::new(AtomicBool::new(false));
    let runner = Ingestix::<(), (), ()>::new((), (), 1, 8);
    let worker = SlowWorker {
        processed: processed.clone(),
    };

    let result = runner.launch(SingleMessageIngestor, worker).await;
    assert!(result.is_ok(), "launch failed: {result:?}");
    assert!(
        processed.load(Ordering::SeqCst),
        "worker was not finished before launch returned"
    );
}

#[tokio::test]
async fn launch_enforces_configured_worker_concurrency() {
    let current = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let worker = ConcurrencyTrackingWorker {
        current: current.clone(),
        max_seen: max_seen.clone(),
    };
    let runner = Ingestix::<usize, (), ()>::new((), (), 2, 32);

    let result = runner.launch(BurstIngestor { count: 20 }, worker).await;
    assert!(result.is_ok(), "launch failed: {result:?}");
    assert!(
        max_seen.load(Ordering::SeqCst) <= 2,
        "max observed concurrency exceeded configured limit: {}",
        max_seen.load(Ordering::SeqCst)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn launch_drains_buffered_messages_after_shutdown_signal() {
    let processed = Arc::new(AtomicUsize::new(0));
    let sent_all = Arc::new(AtomicBool::new(false));
    let message_count = 8;

    let ingestor = BufferedThenWaitIngestor {
        count: message_count,
        sent_all: sent_all.clone(),
    };
    let worker = CountingSlowWorker {
        processed: processed.clone(),
    };
    let runner = Ingestix::<usize, (), ()>::new((), (), 1, 16);
    let launch_task = tokio::spawn(async move { runner.launch(ingestor, worker).await });

    for _ in 0..50 {
        if sent_all.load(Ordering::SeqCst) {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        sent_all.load(Ordering::SeqCst),
        "ingestor did not enqueue all messages in time"
    );

    let signal_result = unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
    assert_eq!(
        signal_result, 0,
        "failed to send SIGTERM to current process"
    );

    let launch_result = timeout(Duration::from_secs(4), launch_task)
        .await
        .expect("launch should complete after SIGTERM")
        .expect("launch task should not panic");
    assert!(
        launch_result.is_ok(),
        "launch returned error after SIGTERM: {launch_result:?}"
    );
    assert_eq!(
        processed.load(Ordering::SeqCst),
        message_count,
        "all buffered messages should be processed before shutdown completes"
    );
}

#[tokio::test]
async fn launch_continues_processing_when_one_worker_call_fails() {
    let ok_count = Arc::new(AtomicUsize::new(0));
    let worker = SelectiveFailWorker {
        ok_count: ok_count.clone(),
        fail_on: 2,
    };
    let runner = Ingestix::<usize, (), ()>::new((), (), 2, 8);
    let ingestor = FixedBatchIngestor {
        values: vec![1, 2, 3],
    };

    let result = runner.launch(ingestor, worker).await;
    assert!(
        result.is_ok(),
        "launch should complete despite worker error"
    );
    assert_eq!(
        ok_count.load(Ordering::SeqCst),
        2,
        "successful messages should still be processed"
    );
}

#[tokio::test]
async fn launch_fail_fast_returns_error_on_worker_failure() {
    let worker = SelectiveFailWorker {
        ok_count: Arc::new(AtomicUsize::new(0)),
        fail_on: 2,
    };
    let runner = Ingestix::<usize, (), ()>::new((), (), 2, 8)
        .with_worker_failure_policy(WorkerFailurePolicy::FailFast);
    let ingestor = FixedBatchIngestor {
        values: vec![1, 2, 3],
    };

    let result = runner.launch(ingestor, worker).await;
    assert!(result.is_err(), "fail-fast should return worker error");
}

#[tokio::test]
async fn launch_fail_fast_returns_error_on_worker_panic() {
    let runner = Ingestix::<usize, (), ()>::new((), (), 1, 8)
        .with_worker_failure_policy(WorkerFailurePolicy::FailFast);
    let ingestor = FixedBatchIngestor { values: vec![1] };

    let result = runner.launch(ingestor, PanicWorker).await;
    assert!(result.is_err(), "fail-fast should return panic join error");
}

#[cfg(feature = "metrics")]
fn reserve_local_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

#[cfg(feature = "metrics")]
async fn get_status_line(addr: std::net::SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to monitor");
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .expect("write monitor request");
    stream.flush().await.expect("flush monitor request");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read monitor response");
    let text = String::from_utf8(raw).expect("utf8 response");
    text.lines().next().expect("status line").to_string()
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn readiness_degrades_after_repeated_worker_failures() {
    let monitor_addr = reserve_local_addr();
    let runner = Ingestix::<usize, (), ()>::new((), (), 1, 8).with_readiness_failure_threshold(1);
    runner
        .spawn_monitor_server(monitor_addr.port())
        .await
        .expect("monitor should start");
    let ingestor = FixedBatchIngestor { values: vec![1] };
    let worker = SelectiveFailWorker {
        ok_count: Arc::new(AtomicUsize::new(0)),
        fail_on: 1,
    };

    let result = runner.launch(ingestor, worker).await;
    assert!(result.is_ok(), "best-effort launch should still complete");

    sleep(Duration::from_millis(50)).await;
    let status = get_status_line(monitor_addr, "/health/ready").await;
    assert!(
        status.starts_with("HTTP/1.1 503"),
        "readiness should degrade after repeated failures, got: {status}"
    );
}

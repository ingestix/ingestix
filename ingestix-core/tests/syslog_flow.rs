#![cfg(feature = "ingestors")]

use ingestix::{Ingestix, SharedContext, SyslogEvent, SyslogIngestor, SyslogProtocol, Worker};
use serial_test::serial;
use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep, timeout};

fn reserve_local_addr() -> std::net::SocketAddr {
    let socket = std::net::UdpSocket::bind(("127.0.0.1", 0)).expect("bind ephemeral UDP port");
    let addr = socket.local_addr().expect("read local addr");
    drop(socket);
    addr
}

struct CollectingWorker {
    events: Arc<Mutex<Vec<SyslogEvent>>>,
}

#[ingestix::async_trait]
impl Worker<SyslogEvent, (), ()> for CollectingWorker {
    async fn process(
        &self,
        msg: SyslogEvent,
        _ctx: Arc<SharedContext<(), ()>>,
    ) -> anyhow::Result<()> {
        self.events.lock().unwrap().push(msg);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn syslog_ingestor_processes_valid_datagrams() {
    let addr = reserve_local_addr();
    let ingestor = SyslogIngestor::new(addr);

    let events = Arc::new(Mutex::new(Vec::<SyslogEvent>::new()));
    let worker = CollectingWorker {
        events: events.clone(),
    };

    let runner = Ingestix::<SyslogEvent, (), ()>::new((), (), 2, 32);
    let launch_task = tokio::spawn(async move { runner.launch(ingestor, worker).await });

    // Give ingestor a brief moment to bind the socket.
    sleep(Duration::from_millis(80)).await;

    let sender = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind UDP sender socket");

    // RFC3164 sample (timestamp without year -> year resolved inside the ingestor).
    let rfc3164 = "<34>Oct 11 22:14:15 mymachine su: hello rfc3164";
    // RFC5424 sample (RFC3339 timestamp with explicit year + timezone).
    let rfc5424 = "<34>1 2026-03-30T22:14:15Z mymachine su 1234 ID47 - hello rfc5424";

    sender
        .send_to(rfc3164.as_bytes(), addr)
        .await
        .expect("send RFC3164 datagram");
    sender
        .send_to(rfc5424.as_bytes(), addr)
        .await
        .expect("send RFC5424 datagram");

    timeout(Duration::from_secs(3), async {
        loop {
            if events.lock().unwrap().len() >= 2 {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker should parse both syslog datagrams");

    let snapshot = events.lock().unwrap().clone();
    let has_rfc3164 = snapshot.iter().any(|ev| {
        matches!(ev.protocol, SyslogProtocol::RFC3164) && ev.message.contains("hello rfc3164")
    });
    let has_rfc5424 = snapshot.iter().any(|ev| {
        matches!(ev.protocol, SyslogProtocol::RFC5424 { .. })
            && ev.message.contains("hello rfc5424")
    });

    assert!(has_rfc3164, "expected a parsed RFC3164 syslog event");
    assert!(has_rfc5424, "expected a parsed RFC5424 syslog event");

    // Trigger Ingestix shutdown (matches patterns used by existing UDP/TCP tests).
    let signal_result = unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
    assert_eq!(signal_result, 0, "failed to send SIGTERM");

    let launch_result = timeout(Duration::from_secs(4), launch_task)
        .await
        .expect("launch should complete after shutdown signal")
        .expect("launch task should not panic");
    assert!(
        launch_result.is_ok(),
        "launch returned error after SIGTERM: {launch_result:?}"
    );
}

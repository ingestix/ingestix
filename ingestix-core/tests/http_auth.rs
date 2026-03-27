#![cfg(feature = "ingestors")]

use ingestix::{ApiKeyConfig, HttpIngestor, Ingestor, SharedContext};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Duration, sleep, timeout};

#[derive(serde::Deserialize)]
struct TestMsg {
    #[allow(dead_code)]
    value: u64,
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

async fn send_request(addr: std::net::SocketAddr, auth_header: Option<&str>, body: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to ingestor");
    let auth_line = auth_header.map(|h| format!("{h}\r\n")).unwrap_or_default();
    let request = format!(
        "POST /ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n{}Connection: close\r\nContent-Length: {}\r\n\r\n{}",
        auth_line,
        body.len(),
        body
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    stream.flush().await.expect("flush request");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    String::from_utf8(raw).expect("utf8 response")
}

async fn run_auth_case(config: ApiKeyConfig, auth_header: Option<&str>, expected_status: &str) {
    let addr = reserve_local_addr();
    let mut ingestor = HttpIngestor::new(addr, "/ingest");
    ingestor.api_key = Some(config);

    let (tx, mut rx) = mpsc::channel::<TestMsg>(8);
    let (shutdown_tx, _) = broadcast::channel(1);
    let ctx = Arc::new(SharedContext {
        config: arc_swap::ArcSwap::from_pointee(()),
        state: (),
        semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
    });

    let mut shutdown_rx = shutdown_tx.subscribe();
    let drain_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                msg = rx.recv() => {
                    if msg.is_none() {
                        break;
                    }
                }
            }
        }
    });

    let shutdown_for_server = shutdown_tx.clone();
    let server_task = tokio::spawn(async move {
        ingestor
            .run(tx, ctx, shutdown_for_server.subscribe())
            .await
            .expect("http ingestor should run");
    });

    wait_until_listening(addr).await;
    let response = send_request(addr, auth_header, "{\"value\":1}").await;
    assert!(
        response.starts_with(expected_status),
        "expected {expected_status}, got response: {response}"
    );

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server task should exit")
        .expect("server task join should succeed");
    let _ = drain_task.await;
}

#[tokio::test]
async fn http_auth_matrix() {
    run_auth_case(
        ApiKeyConfig::x_api_key("secret"),
        Some("x-api-key: secret"),
        "HTTP/1.1 202",
    )
    .await;

    run_auth_case(ApiKeyConfig::x_api_key("secret"), None, "HTTP/1.1 401").await;

    run_auth_case(
        ApiKeyConfig::bearer_auth("token-123"),
        Some("authorization: bearer token-123"),
        "HTTP/1.1 202",
    )
    .await;

    run_auth_case(
        ApiKeyConfig::basic_auth("dXNlcjpwYXNz"),
        Some("authorization: Basic"),
        "HTTP/1.1 401",
    )
    .await;
}

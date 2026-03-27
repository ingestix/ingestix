#![cfg(feature = "ingestors")]

use ingestix::{HttpIngestor, Ingestor, SharedContext};
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

async fn write_request(stream: &mut TcpStream, request: &str) {
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    stream.flush().await.expect("flush request");
}

async fn read_http_response_head(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1];
    loop {
        let n = stream.read(&mut tmp).await.expect("read response");
        assert!(n > 0, "connection closed before response headers");
        buf.push(tmp[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(buf).expect("response headers must be utf8")
}

async fn drain_http_body_if_present(stream: &mut TcpStream, headers: &str) {
    let lower = headers.to_ascii_lowercase();
    let content_length = lower
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map(|v| v.trim().parse::<usize>().expect("valid content-length"))
        .unwrap_or(0);

    if content_length > 0 {
        let mut body = vec![0u8; content_length];
        stream
            .read_exact(&mut body)
            .await
            .expect("read response body");
    }
}

#[tokio::test]
async fn http_returns_503_after_shutdown_is_triggered() {
    let addr = reserve_local_addr();
    let ingestor = HttpIngestor::new(addr, "/ingest");
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
            .expect("http ingestor run should succeed");
    });

    wait_until_listening(addr).await;
    let mut stream = TcpStream::connect(addr).await.expect("connect to ingestor");

    let first_req = "POST /ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nConnection: keep-alive\r\nContent-Length: 11\r\n\r\n{\"value\":1}";
    write_request(&mut stream, first_req).await;
    let first_headers = read_http_response_head(&mut stream).await;
    assert!(
        first_headers.starts_with("HTTP/1.1 202"),
        "expected first response 202, got headers: {first_headers}"
    );
    drain_http_body_if_present(&mut stream, &first_headers).await;

    let _ = shutdown_tx.send(());
    sleep(Duration::from_millis(50)).await;

    let second_req = "POST /ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: 11\r\n\r\n{\"value\":2}";
    write_request(&mut stream, second_req).await;
    let second_headers = read_http_response_head(&mut stream).await;
    assert!(
        second_headers.starts_with("HTTP/1.1 503"),
        "expected second response 503 during shutdown, got headers: {second_headers}"
    );
    drain_http_body_if_present(&mut stream, &second_headers).await;

    timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server task should finish after shutdown")
        .expect("server task join should succeed");
    let _ = drain_task.await;
}

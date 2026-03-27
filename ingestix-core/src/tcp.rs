use crate::{
    INGESTED_MSGS, Ingestor, RECEIVED_MSGS, REJECTED_INVALID_JSON, REJECTED_OVERSIZED_PAYLOAD,
    REJECTED_REQUEST_TIMEOUT, SharedContext,
};
use futures::StreamExt;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, broadcast, mpsc};
use tokio::time::{Duration, timeout};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

#[derive(Clone, Debug)]
/// TCP ingestor tuning knobs.
pub struct TcpConfig {
    /// Maximum number of concurrent accepted TCP connections.
    ///
    /// Default: `1024`.
    pub max_connections: usize,
    /// Per-connection idle read timeout.
    ///
    /// When no frame arrives within this duration, the connection is closed and
    /// a timeout rejection metric is incremented.
    ///
    /// Default: `30s`.
    pub read_idle_timeout: Duration,
    /// Maximum size, in bytes, accepted by the length-delimited codec.
    ///
    /// Default: `1 MiB`.
    pub max_frame_length: usize,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            read_idle_timeout: Duration::from_secs(30),
            max_frame_length: 1024 * 1024,
        }
    }
}

/// TCP ingestion server that reads length-delimited frames and forwards decoded messages into Ingestix.
///
/// Messages are interpreted as JSON and must deserialize into `M`.
pub struct TcpIngestor {
    /// Socket address to bind for the TCP server.
    pub addr: std::net::SocketAddr,
    /// Maximum number of concurrent accepted connections.
    pub max_connections: usize,
    /// Per-connection idle read timeout.
    pub read_idle_timeout: Duration,
    /// Maximum frame size accepted by the length-delimited codec.
    pub max_frame_length: usize,
}

impl TcpIngestor {
    /// Create a TCP ingestor using [`TcpConfig::default`].
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self::with_config(addr, TcpConfig::default())
    }

    /// Create a TCP ingestor with explicit configuration.
    pub fn with_config(addr: std::net::SocketAddr, config: TcpConfig) -> Self {
        Self {
            addr,
            max_connections: config.max_connections,
            read_idle_timeout: config.read_idle_timeout,
            max_frame_length: config.max_frame_length,
        }
    }
}

#[async_trait::async_trait]
impl<M, C, S> Ingestor<M, C, S> for TcpIngestor
where
    M: for<'a> serde::Deserialize<'a> + Send + 'static,
    C: Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    async fn run(
        self,
        tx: mpsc::Sender<M>,
        _ctx: Arc<SharedContext<C, S>>,
        mut shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        let connection_slots = Arc::new(Semaphore::new(self.max_connections));
        let read_idle_timeout = self.read_idle_timeout;
        let max_frame_length = self.max_frame_length;
        crate::flow_log_info!("TCP Ingestor listening on {}", self.addr);

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    crate::flow_log_info!("TCP Ingestor shutting down");
                    break;
                }
                accept_res = listener.accept() => {
                    let (stream, _peer) = accept_res?;
                    let tx_inner = tx.clone();
                    let mut conn_shutdown = shutdown.resubscribe();
                    let permit = tokio::select! {
                        _ = shutdown.recv() => break,
                        permit_res = connection_slots.clone().acquire_owned() => {
                            match permit_res {
                                Ok(permit) => permit,
                                Err(_) => break,
                            }
                        }
                    };

                    tokio::spawn(async move {
                        let _permit = permit;
                        let codec = LengthDelimitedCodec::builder()
                            .max_frame_length(max_frame_length)
                            .new_codec();
                        let mut framed = FramedRead::new(stream, codec);
                        loop {
                            tokio::select! {
                                _ = conn_shutdown.recv() => break,
                                frame = timeout(read_idle_timeout, framed.next()) => {
                                    let Ok(frame) = frame else {
                                        REJECTED_REQUEST_TIMEOUT.inc();
                                        crate::flow_log_debug!("TCP idle timeout for {}", _peer);
                                        break;
                                    };
                                    let Some(frame) = frame else {
                                        break;
                                    };
                                    let bytes = match frame {
                                        Ok(bytes) => bytes,
                                        Err(_) => {
                                            REJECTED_OVERSIZED_PAYLOAD.inc();
                                            break;
                                        }
                                    };
                                    if let Ok(msg) = serde_json::from_slice::<M>(&bytes) {
                                        RECEIVED_MSGS.inc();
                                        tokio::select! {
                                            _ = conn_shutdown.recv() => break,
                                            send_res = tx_inner.send(msg) => {
                                                if send_res.is_ok() {
                                                    INGESTED_MSGS.inc();
                                                } else {
                                                    break;
                                                }
                                            }
                                        }
                                    } else {
                                        REJECTED_INVALID_JSON.inc();
                                    }
                                }
                            }
                        }
                        crate::flow_log_debug!("Connection closed for {}", _peer);
                    });
                }
            }
        }
        Ok(())
    }
}

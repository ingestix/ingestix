use crate::{
    INGESTED_MSGS, Ingestor, RECEIVED_MSGS, REJECTED_INVALID_JSON, REJECTED_OVERSIZED_PAYLOAD,
    SharedContext,
};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc};

#[derive(Clone, Debug)]
/// UDP ingestor tuning knobs.
pub struct UdpConfig {
    /// Maximum datagram payload size accepted for JSON deserialization.
    ///
    /// Larger datagrams are rejected and counted as oversized payloads.
    ///
    /// Default: `65535`.
    pub max_datagram_size: usize,
    /// Size of the internal receive buffer used by `recv_from`.
    ///
    /// Keep this at least as large as `max_datagram_size` to avoid truncation.
    ///
    /// Default: `65535`.
    pub recv_buffer_size: usize,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            max_datagram_size: 65535,
            recv_buffer_size: 65535,
        }
    }
}

/// UDP ingestion server that receives datagrams and forwards decoded messages into Ingestix.
///
/// Each datagram is interpreted as JSON and must deserialize into `M`.
pub struct UdpIngestor {
    /// Socket address to bind for the UDP server.
    pub addr: std::net::SocketAddr,
    /// Maximum accepted datagram payload size (in bytes).
    pub max_datagram_size: usize,
    /// Size of the internal receive buffer used by `recv_from`.
    pub recv_buffer_size: usize,
}

impl UdpIngestor {
    /// Create a UDP ingestor using [`UdpConfig::default`].
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self::with_config(addr, UdpConfig::default())
    }

    /// Create a UDP ingestor with explicit configuration.
    pub fn with_config(addr: std::net::SocketAddr, config: UdpConfig) -> Self {
        Self {
            addr,
            max_datagram_size: config.max_datagram_size,
            recv_buffer_size: config.recv_buffer_size,
        }
    }
}

#[async_trait::async_trait]
impl<M, C, S> Ingestor<M, C, S> for UdpIngestor
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
        let socket = UdpSocket::bind(self.addr).await?;
        let mut buf = vec![0u8; self.recv_buffer_size];
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                res = socket.recv_from(&mut buf) => {
                    let (len, _) = res?;
                    if len > self.max_datagram_size {
                        REJECTED_OVERSIZED_PAYLOAD.inc();
                        continue;
                    }
                    if let Ok(msg) = serde_json::from_slice::<M>(&buf[..len]) {
                        RECEIVED_MSGS.inc();
                        tokio::select! {
                            _ = shutdown.recv() => break,
                            send_res = tx.send(msg) => {
                                if send_res.is_ok() {
                                    INGESTED_MSGS.inc();
                                }
                            }
                        }
                    } else {
                        REJECTED_INVALID_JSON.inc();
                    }
                }
            }
        }
        Ok(())
    }
}

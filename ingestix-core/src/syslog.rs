use crate::{
    INGESTED_MSGS, Ingestor, RECEIVED_MSGS, REJECTED_INVALID_SYSLOG, REJECTED_OVERSIZED_PAYLOAD,
    SharedContext,
};
use chrono::Datelike;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc};

/// Parsed RFC3164/RFC5424 syslog information.
///
/// Notes:
/// - For MVP we keep the event fields “serde-friendly” as `String`/`Option<String>`.
/// - Transport is handled by [`SyslogIngestor`]; parsing happens inside the ingestor.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SyslogEvent {
    pub raw: String,
    pub protocol: SyslogProtocol,
    pub facility: Option<String>,
    pub severity: Option<String>,
    pub timestamp: Option<String>,
    pub hostname: Option<String>,
    pub appname: Option<String>,
    pub procid: Option<String>,
    pub msgid: Option<String>,
    pub structured_data: Vec<SyslogStructuredDataElement>,
    pub message: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SyslogProtocol {
    RFC3164,
    RFC5424 { version: u32 },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SyslogStructuredDataElement {
    pub id: String,
    pub params: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
/// Syslog ingestion tuning knobs.
pub struct SyslogConfig {
    /// Maximum datagram payload size accepted for parsing.
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

impl Default for SyslogConfig {
    fn default() -> Self {
        Self {
            max_datagram_size: 65535,
            recv_buffer_size: 65535,
        }
    }
}

/// UDP ingestor that receives syslog lines, parses them, and forwards decoded
/// events into Ingestix.
pub struct SyslogIngestor {
    /// Socket address to bind for the UDP server.
    pub addr: std::net::SocketAddr,
    /// Maximum accepted datagram payload size in bytes.
    pub max_datagram_size: usize,
    /// Size of the internal receive buffer used by `recv_from`.
    pub recv_buffer_size: usize,
}

impl SyslogIngestor {
    /// Create a Syslog ingestor using [`SyslogConfig::default`].
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self::with_config(addr, SyslogConfig::default())
    }

    /// Create a Syslog ingestor with explicit configuration.
    pub fn with_config(addr: std::net::SocketAddr, config: SyslogConfig) -> Self {
        Self {
            addr,
            max_datagram_size: config.max_datagram_size,
            recv_buffer_size: config.recv_buffer_size,
        }
    }
}

fn resolve_year(_: syslog_loose::IncompleteDate) -> i32 {
    // Used for RFC3164 year resolution in `parse_message_with_year_exact`.
    // We intentionally avoid any "near-midnight" heuristics for the MVP.
    chrono::Utc::now().year()
}

/// Transport-agnostic syslog parsing: bytes/transport logic should extract a
/// single syslog line and then call this helper.
///
/// This separation is intentional so a future QUIC ingestor can reuse the same
/// "parse -> enqueue" semantics while swapping only the transport receive loop.
fn parse_syslog_line(raw_line: &str) -> Option<SyslogEvent> {
    // MVP parsing strategy:
    // 1) Attempt RFC5424 exact parse
    // 2) Fallback to RFC3164 exact parse
    let msg_5424 = syslog_loose::parse_message_with_year_exact(
        raw_line,
        resolve_year,
        syslog_loose::Variant::RFC5424,
    );
    if let Ok(parsed) = msg_5424 {
        return Some(SyslogEvent::from_syslog_message(raw_line, parsed));
    }

    let msg_3164 = syslog_loose::parse_message_with_year_exact(
        raw_line,
        resolve_year,
        syslog_loose::Variant::RFC3164,
    );
    let parsed_3164 = msg_3164.ok()?;
    Some(SyslogEvent::from_syslog_message(raw_line, parsed_3164))
}

impl SyslogEvent {
    fn from_syslog_message(raw_line: &str, msg: syslog_loose::Message<&str>) -> Self {
        let protocol = match msg.protocol {
            syslog_loose::Protocol::RFC3164 => SyslogProtocol::RFC3164,
            syslog_loose::Protocol::RFC5424(version) => SyslogProtocol::RFC5424 { version },
        };

        let facility = msg.facility.map(|f| f.as_str().to_string());
        let severity = msg.severity.map(|s| s.as_str().to_string());
        let timestamp = msg.timestamp.map(|ts| ts.to_rfc3339());
        let hostname = msg.hostname.map(|h| h.to_string());
        let appname = msg.appname.map(|a| a.to_string());
        let procid = msg.procid.map(|p| match p {
            syslog_loose::ProcId::PID(pid) => pid.to_string(),
            syslog_loose::ProcId::Name(name) => name.to_string(),
        });
        let msgid = msg.msgid.map(|m| m.to_string());

        let structured_data = msg
            .structured_data
            .into_iter()
            .map(|el| SyslogStructuredDataElement {
                id: el.id.to_string(),
                params: el
                    .params
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            })
            .collect();

        let message = msg.msg.to_string();

        Self {
            raw: raw_line.to_string(),
            protocol,
            facility,
            severity,
            timestamp,
            hostname,
            appname,
            procid,
            msgid,
            structured_data,
            message,
        }
    }
}

#[async_trait::async_trait]
impl<C, S> Ingestor<SyslogEvent, C, S> for SyslogIngestor
where
    C: Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    async fn run(
        self,
        tx: mpsc::Sender<SyslogEvent>,
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

                    let raw = match std::str::from_utf8(&buf[..len]) {
                        Ok(v) => v,
                        Err(_) => {
                            // MVP: we only parse UTF-8 syslog lines.
                            REJECTED_INVALID_SYSLOG.inc();
                            continue;
                        }
                    };

                    // Syslog line may include CRLF endings; remove those for parsing.
                    // Then we delegate the actual parsing into `parse_syslog_line`.
                    let line = raw.trim_end_matches(&['\r', '\n'][..]);

                    let event = match parse_syslog_line(line) {
                        Some(ev) => ev,
                        None => {
                            REJECTED_INVALID_SYSLOG.inc();
                            continue;
                        }
                    };

                    RECEIVED_MSGS.inc();
                    tokio::select! {
                        _ = shutdown.recv() => break,
                        send_res = tx.send(event) => {
                            if send_res.is_ok() {
                                INGESTED_MSGS.inc();
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syslog_event_smoke_parse_rfc5424() {
        // This is a lightweight unit test (not dependent on UDP transport).
        // The year resolution is handled internally by `parse_message_with_year_exact`.
        let line = "<34>1 2026-03-30T22:14:15Z mymachine appname 1234 ID47 - hello world";
        let ev = parse_syslog_line(line).expect("should parse RFC5424");
        assert!(matches!(ev.protocol, SyslogProtocol::RFC5424 { .. }));
        assert_eq!(ev.message, "hello world");
    }
}


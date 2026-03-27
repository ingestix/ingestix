use crate::{
    INGESTED_MSGS, Ingestor, RECEIVED_MSGS, REJECTED_INVALID_API_KEY, REJECTED_INVALID_JSON,
    REJECTED_OVERLOADED_QUEUE, REJECTED_OVERSIZED_PAYLOAD, REJECTED_REQUEST_TIMEOUT, SharedContext,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use subtle::ConstantTimeEq;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Duration, sleep, timeout};

#[derive(Clone, Copy, Debug)]
/// Strategy used when the internal queue cannot accept new messages immediately.
pub enum HttpQueuePolicy {
    /// Wait for queue capacity until `HttpConfig::enqueue_timeout` elapses.
    Block,
    /// Reject immediately with HTTP `503 Service Unavailable` when queue is full.
    Reject503,
    /// Reject immediately with HTTP `429 Too Many Requests` when queue is full.
    Reject429,
}

#[derive(Clone, Debug)]
/// HTTP ingestor tuning knobs.
///
/// Use with [`HttpIngestor::with_config`] for explicit behavior, or rely on
/// [`Default`] via [`HttpIngestor::new`] for minimal setup.
pub struct HttpConfig {
    /// Require `Content-Type: application/json` on incoming requests.
    ///
    /// Default: `true`.
    pub require_json: bool,
    /// Optional API-key or auth-token validation applied before deserialization.
    ///
    /// Default: `None`.
    pub api_key: Option<ApiKeyConfig>,
    /// Maximum accepted request body size in bytes.
    ///
    /// Default: `1 MiB`.
    pub max_body_bytes: usize,
    /// Upper bound for total request handling time before returning timeout error.
    ///
    /// Default: `10s`.
    pub request_timeout: Duration,
    /// Maximum time to wait while enqueueing a message when policy is `Block`.
    ///
    /// Default: `2s`.
    pub enqueue_timeout: Duration,
    /// Queue saturation policy used when workers cannot keep up.
    ///
    /// Default: [`HttpQueuePolicy::Reject503`].
    pub queue_policy: HttpQueuePolicy,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            require_json: true,
            api_key: None,
            max_body_bytes: 1024 * 1024,
            request_timeout: Duration::from_secs(10),
            enqueue_timeout: Duration::from_secs(2),
            queue_policy: HttpQueuePolicy::Reject503,
        }
    }
}

#[derive(Clone)]
/// API key / token validation rule for HTTP ingestion.
pub struct ApiKeyConfig {
    /// Header name to read from (case-insensitive as per HTTP semantics).
    pub header_name: String,
    /// Expected secret value compared using constant-time equality.
    ///
    /// Matching is strict: the incoming header/token must match exactly
    /// (no implicit trimming of surrounding whitespace).
    pub expected_value: String,
    /// Optional auth scheme prefix (for example `Bearer` or `Basic`).
    pub scheme: Option<String>,
}

impl std::fmt::Debug for ApiKeyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyConfig")
            .field("header_name", &self.header_name)
            .field("expected_value", &"***redacted***")
            .field("scheme", &self.scheme)
            .finish()
    }
}

impl ApiKeyConfig {
    /// Build config for `x-api-key: <value>`.
    pub fn x_api_key(expected_value: impl Into<String>) -> Self {
        Self {
            header_name: "x-api-key".to_string(),
            expected_value: expected_value.into(),
            scheme: None,
        }
    }

    /// Build config for a custom header carrying the secret as raw value.
    pub fn header(header_name: impl Into<String>, expected_value: impl Into<String>) -> Self {
        Self {
            header_name: header_name.into(),
            expected_value: expected_value.into(),
            scheme: None,
        }
    }

    /// Build config for `Authorization: Bearer <token>`.
    pub fn bearer_auth(expected_token: impl Into<String>) -> Self {
        Self {
            header_name: "authorization".to_string(),
            expected_value: expected_token.into(),
            scheme: Some("Bearer".to_string()),
        }
    }

    /// Build config for `Authorization: Basic <credentials>`.
    pub fn basic_auth(expected_credentials: impl Into<String>) -> Self {
        Self {
            header_name: "authorization".to_string(),
            expected_value: expected_credentials.into(),
            scheme: Some("Basic".to_string()),
        }
    }
}

struct HttpIngestorState<M> {
    tx: mpsc::Sender<M>,
    require_json: bool,
    api_key: Option<ApiKeyConfig>,
    accepting: Arc<AtomicBool>,
    max_body_bytes: usize,
    queue_policy: HttpQueuePolicy,
    enqueue_timeout: Duration,
}

impl<M> Clone for HttpIngestorState<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            require_json: self.require_json,
            api_key: self.api_key.clone(),
            accepting: self.accepting.clone(),
            max_body_bytes: self.max_body_bytes,
            queue_policy: self.queue_policy,
            enqueue_timeout: self.enqueue_timeout,
        }
    }
}

/// HTTP ingestion server that accepts JSON payloads and pushes decoded messages into Ingestix.
///
/// The ingestor is configured with:
/// - `require_json` and `max_body_bytes` for request validation,
/// - optional API key/token validation via [`ApiKeyConfig`],
/// - `queue_policy` to decide what happens when the core queue is full.
pub struct HttpIngestor {
    /// Socket address to bind for the HTTP server.
    pub addr: std::net::SocketAddr,
    /// URL path (for example `"/v1/ingest"`).
    pub path: String,
    /// When `true`, rejects requests whose `Content-Type` is not `application/json`.
    pub require_json: bool,
    /// Optional API key/token validation applied before JSON deserialization.
    pub api_key: Option<ApiKeyConfig>,
    /// Maximum accepted request body size.
    pub max_body_bytes: usize,
    /// Upper bound for total request handling time.
    pub request_timeout: Duration,
    /// Maximum time to wait while enqueueing a message when [`HttpQueuePolicy::Block`] is selected.
    pub enqueue_timeout: Duration,
    /// Queue saturation policy used when workers cannot keep up.
    pub queue_policy: HttpQueuePolicy,
}

impl HttpIngestor {
    /// Create an HTTP ingestor using [`HttpConfig::default`].
    pub fn new(addr: std::net::SocketAddr, path: impl Into<String>) -> Self {
        Self::with_config(addr, path, HttpConfig::default())
    }

    /// Create an HTTP ingestor with explicit configuration.
    pub fn with_config(
        addr: std::net::SocketAddr,
        path: impl Into<String>,
        config: HttpConfig,
    ) -> Self {
        Self {
            addr,
            path: path.into(),
            require_json: config.require_json,
            api_key: config.api_key,
            max_body_bytes: config.max_body_bytes,
            request_timeout: config.request_timeout,
            enqueue_timeout: config.enqueue_timeout,
            queue_policy: config.queue_policy,
        }
    }
}

#[async_trait::async_trait]
impl<M, C, S> Ingestor<M, C, S> for HttpIngestor
where
    M: serde::de::DeserializeOwned + Send + 'static,
    C: Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    async fn run(
        self,
        tx: mpsc::Sender<M>,
        _ctx: Arc<SharedContext<C, S>>,
        mut shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let accepting = Arc::new(AtomicBool::new(true));
        let state = HttpIngestorState {
            tx,
            require_json: self.require_json,
            api_key: self.api_key.clone(),
            accepting: accepting.clone(),
            max_body_bytes: self.max_body_bytes,
            queue_policy: self.queue_policy,
            enqueue_timeout: self.enqueue_timeout,
        };
        let request_timeout = self.request_timeout;

        let app = Router::new()
            .route(
                &self.path,
                post(
                    move |State(state): State<HttpIngestorState<M>>,
                          headers: HeaderMap,
                          body: Bytes| async move {
                        let response = timeout(
                            request_timeout,
                            process_http_ingest::<M>(state, headers, body),
                        )
                        .await;
                        match response {
                            Ok(resp) => resp,
                            Err(_) => {
                                REJECTED_REQUEST_TIMEOUT.inc();
                                json_error(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "request_timeout",
                                    "Request timed out",
                                )
                            }
                        }
                    },
                ),
            )
            .layer(DefaultBodyLimit::max(self.max_body_bytes))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        crate::flow_log_info!(
            "HTTP Ingestor listening on {} at path {}",
            self.addr,
            self.path
        );

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown.recv().await;
                accepting.store(false, Ordering::SeqCst);
                // Keep the listener alive briefly so requests racing with shutdown
                // are rejected at the handler boundary with 503.
                sleep(Duration::from_millis(100)).await;
                crate::flow_log_info!("HTTP Ingestor received shutdown signal");
            })
            .await?;

        Ok(())
    }
}

async fn process_http_ingest<M>(
    state: HttpIngestorState<M>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    M: serde::de::DeserializeOwned + Send + 'static,
{
    if !state.accepting.load(Ordering::SeqCst) {
        REJECTED_OVERLOADED_QUEUE.inc();
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "Server is shutting down",
        );
    }

    if state.require_json && !is_json_content_type(&headers) {
        REJECTED_INVALID_JSON.inc();
        return json_error_with_accept(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "Content-Type must be application/json",
        );
    }

    if let Some(auth) = &state.api_key
        && !is_valid_api_key(&headers, auth)
    {
        REJECTED_INVALID_API_KEY.inc();
        return json_error(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "API key is missing or invalid",
        );
    }

    if body.len() > state.max_body_bytes {
        REJECTED_OVERSIZED_PAYLOAD.inc();
        return json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "Request body exceeds configured size limit",
        );
    }

    let msg: M = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            REJECTED_INVALID_JSON.inc();
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "Request body is not valid JSON for this endpoint",
            );
        }
    };
    RECEIVED_MSGS.inc();

    match state.queue_policy {
        HttpQueuePolicy::Block => match timeout(state.enqueue_timeout, state.tx.send(msg)).await {
            Ok(Ok(_)) => {
                INGESTED_MSGS.inc();
                StatusCode::ACCEPTED.into_response()
            }
            Ok(Err(_)) => json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Failed to enqueue message",
            ),
            Err(_) => {
                REJECTED_OVERLOADED_QUEUE.inc();
                json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "queue_overloaded",
                    "Queue is saturated, retry later",
                )
            }
        },
        HttpQueuePolicy::Reject503 => match state.tx.try_send(msg) {
            Ok(()) => {
                INGESTED_MSGS.inc();
                StatusCode::ACCEPTED.into_response()
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                REJECTED_OVERLOADED_QUEUE.inc();
                json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "queue_overloaded",
                    "Queue is saturated, retry later",
                )
            }
            Err(mpsc::error::TrySendError::Closed(_)) => json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Failed to enqueue message",
            ),
        },
        HttpQueuePolicy::Reject429 => match state.tx.try_send(msg) {
            Ok(()) => {
                INGESTED_MSGS.inc();
                StatusCode::ACCEPTED.into_response()
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                REJECTED_OVERLOADED_QUEUE.inc();
                json_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "queue_overloaded",
                    "Too many requests",
                )
            }
            Err(mpsc::error::TrySendError::Closed(_)) => json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Failed to enqueue message",
            ),
        },
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };
    let Ok(content_type) = content_type.to_str() else {
        return false;
    };
    let mime = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    mime == "application/json"
}

fn is_valid_api_key(headers: &HeaderMap, config: &ApiKeyConfig) -> bool {
    let Some(raw_header) = headers.get(config.header_name.as_str()) else {
        return false;
    };
    let Ok(raw_header) = raw_header.to_str() else {
        return false;
    };

    match &config.scheme {
        Some(scheme) => {
            let Some((provided_scheme, provided_value)) = raw_header.split_once(' ') else {
                return false;
            };
            let scheme_ok = provided_scheme.eq_ignore_ascii_case(scheme);
            scheme_ok && secure_eq(provided_value.as_bytes(), config.expected_value.as_bytes())
        }
        None => secure_eq(raw_header.as_bytes(), config.expected_value.as_bytes()),
    }
}

fn secure_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

#[derive(serde::Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(serde::Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: &'static str,
}

fn json_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (
        status,
        axum::Json(ErrorBody {
            error: ErrorDetail { code, message },
        }),
    )
        .into_response()
}

fn json_error_with_accept(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
) -> Response {
    let accept_header = [(header::ACCEPT, HeaderValue::from_static("application/json"))];
    (
        status,
        accept_header,
        axum::Json(ErrorBody {
            error: ErrorDetail { code, message },
        }),
    )
        .into_response()
}

// Copyright (c) 2026 Magnus Karlsson and Ingestix contributors
// Licensed under the Apache License, Version 2.0 or the MIT license.

//! # Ingestix Core
//!
//! A small "glue" micro-framework for building high-throughput ingestion pipelines.
//!
//! Ingestix wires together two concepts:
//! - An [`Ingestor`] that listens for incoming data and pushes messages into a bounded queue.
//! - A [`Worker`] that pulls from that queue and processes messages concurrently.
//!
//! The main orchestration point is [`Ingestix::launch`].
//!
//! ## Quick mental model
//! - `Ingestor::run` runs in the background and calls `tx.send(msg)` whenever it receives one.
//! - `Worker::process` gets invoked for each queued message (up to your configured concurrency).
//! - Failures follow a [`WorkerFailurePolicy`] (`BestEffort` vs `FailFast`).
//!
//! ## Feature flags
//! - `ingestors` enables the built-in HTTP/TCP/UDP ingestors (modules: `http`, `tcp`, `udp`).
//! - `derive` re-exports `FlowWorker` from the `ingestix-derive` proc-macro crate for generating `Worker` impls.
//! - `metrics` enables the Prometheus monitor server (`/metrics`, `/health/*`).
//!
//! ## Doctest note
//! The public API is documented inline, but examples are intentionally marked `ignore`
//! because they depend on your application-level message/config/state types.

use arc_swap::ArcSwap;
#[cfg(feature = "metrics")]
use axum::{Router, http::StatusCode, routing::get};
use std::sync::Arc;
#[cfg(feature = "metrics")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Semaphore, broadcast, mpsc};
use tokio::task::JoinSet;

pub use async_trait::async_trait;
#[cfg(feature = "derive")]
pub use ingestix_derive::FlowWorker;

#[cfg(feature = "ingestors")]
pub mod http;
#[cfg(feature = "ingestors")]
pub mod tcp;
#[cfg(feature = "ingestors")]
pub mod udp;

#[cfg(feature = "ingestors")]
pub use http::{ApiKeyConfig, HttpConfig, HttpIngestor, HttpQueuePolicy};
#[cfg(feature = "ingestors")]
pub use tcp::{TcpConfig, TcpIngestor};
#[cfg(feature = "ingestors")]
pub use udp::{UdpConfig, UdpIngestor};

#[cfg(feature = "logging")]
#[allow(unused_macros)]
macro_rules! flow_log_info {
    ($($arg:tt)*) => {
        tracing::info!($($arg)*);
    };
}

#[cfg(not(feature = "logging"))]
#[allow(unused_macros)]
macro_rules! flow_log_info {
    ($($arg:tt)*) => {{};};
}

#[cfg(feature = "logging")]
#[allow(unused_macros)]
macro_rules! flow_log_debug {
    ($($arg:tt)*) => {
        tracing::debug!($($arg)*);
    };
}

#[cfg(not(feature = "logging"))]
#[allow(unused_macros)]
macro_rules! flow_log_debug {
    ($($arg:tt)*) => {{};};
}

#[allow(unused_imports)]
pub(crate) use flow_log_debug;
#[allow(unused_imports)]
pub(crate) use flow_log_info;

#[cfg(feature = "logging")]
#[allow(unused_macros)]
macro_rules! flow_log_warn {
    ($($arg:tt)*) => {
        tracing::warn!($($arg)*);
    };
}

#[cfg(not(feature = "logging"))]
#[allow(unused_macros)]
macro_rules! flow_log_warn {
    ($($arg:tt)*) => {{};};
}

#[allow(unused_imports)]
pub(crate) use flow_log_warn;

#[cfg(feature = "metrics")]
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

#[cfg(feature = "metrics")]
static PROM_HANDLE: OnceLock<Option<PrometheusHandle>> = OnceLock::new();

#[cfg(feature = "metrics")]
fn maybe_prom_handle() -> Option<&'static PrometheusHandle> {
    PROM_HANDLE
        .get_or_init(|| match PrometheusBuilder::new().install_recorder() {
            Ok(handle) => {
                metrics::counter!("flow_received_total").increment(0);
                metrics::counter!("flow_ingested_total").increment(0);
                metrics::counter!("flow_processed_total").increment(0);
                metrics::gauge!("flow_queue_depth").set(0.0);
                Some(handle)
            }
            Err(err) => {
                crate::flow_log_warn!("Prometheus recorder not installed: {}", err);
                None
            }
        })
        .as_ref()
}

#[derive(Clone, Copy)]
/// A lightweight wrapper around a metric name.
///
/// These names are used with the built-in metric helpers. When the `metrics` feature
/// is disabled, they become no-ops so the core logic can run without observability
/// dependencies.
pub struct FlowCounterName(&'static str);

impl FlowCounterName {
    pub fn inc(&self) {
        #[cfg(feature = "metrics")]
        metrics::counter!(self.0).increment(1);

        #[cfg(not(feature = "metrics"))]
        let _ = self.0;
    }
}

#[derive(Clone, Copy)]
/// A lightweight wrapper around a metric gauge name.
///
/// These names are used with the built-in metric helpers. When the `metrics` feature
/// is disabled, they become no-ops.
pub struct FlowGaugeName(&'static str);

impl FlowGaugeName {
    pub fn set(&self, value: i64) {
        #[cfg(feature = "metrics")]
        metrics::gauge!(self.0).set(value as f64);

        #[cfg(not(feature = "metrics"))]
        let _ = (self.0, value);
    }
}

/// Total successfully enqueued messages (ingestor -> core queue).
pub const INGESTED_MSGS: FlowCounterName = FlowCounterName("flow_ingested_total");

/// Total messages successfully processed by workers.
pub const PROCESSED_MSGS: FlowCounterName = FlowCounterName("flow_processed_total");

/// Total messages received by ingestors (before any validation / enqueueing).
pub const RECEIVED_MSGS: FlowCounterName = FlowCounterName("flow_received_total");

/// Current internal queue depth (best-effort gauge).
pub const CURRENT_QUEUE: FlowGaugeName = FlowGaugeName("flow_queue_depth");

/// Messages rejected due to invalid JSON decoding.
pub const REJECTED_INVALID_JSON: FlowCounterName =
    FlowCounterName("flow_rejected_invalid_json_total");

/// Requests rejected because an API key / token didn't match.
pub const REJECTED_INVALID_API_KEY: FlowCounterName =
    FlowCounterName("flow_rejected_invalid_api_key_total");

/// Messages rejected because the payload exceeded the configured maximum size.
pub const REJECTED_OVERSIZED_PAYLOAD: FlowCounterName =
    FlowCounterName("flow_rejected_oversized_payload_total");

/// Requests rejected due to handler/reader timeout.
pub const REJECTED_REQUEST_TIMEOUT: FlowCounterName =
    FlowCounterName("flow_rejected_request_timeout_total");

/// Messages rejected because the internal queue was saturated.
pub const REJECTED_OVERLOADED_QUEUE: FlowCounterName =
    FlowCounterName("flow_rejected_overloaded_queue_total");

/// Shared runtime context shared between ingestor and worker.
///
/// - `config` is stored in an [`arc_swap::ArcSwap`], which makes it cheap to
///   replace the configuration at runtime without locking the hot path.
/// - `state` is your application state (passed by reference via `Arc` ownership
///   on the `SharedContext` itself).
/// - `semaphore` controls worker concurrency.
pub struct SharedContext<C, S> {
    pub config: ArcSwap<C>,
    pub state: S,
    pub semaphore: Arc<Semaphore>,
}

#[async_trait]
/// Produces messages for the core loop.
///
/// An ingestor is responsible for listening (HTTP, TCP, UDP, etc.), validating input,
/// and pushing decoded messages into the bounded queue via `tx`.
pub trait Ingestor<M, C, S>: Send + 'static {
    async fn run(
        self,
        tx: mpsc::Sender<M>,
        ctx: Arc<SharedContext<C, S>>,
        shutdown: broadcast::Receiver<()>,
    ) -> anyhow::Result<()>;
}

#[async_trait]
/// Processes queued messages.
///
/// A worker gets called concurrently for each message pulled from the core queue.
/// Use `ctx` to read the current configuration and access shared state.
pub trait Worker<M, C, S>: Send + Sync + 'static {
    async fn process(&self, msg: M, ctx: Arc<SharedContext<C, S>>) -> anyhow::Result<()>;
}

#[derive(Clone, Copy)]
/// What should the core do when workers start failing.
pub enum WorkerFailurePolicy {
    BestEffort,
    FailFast,
}

/// Main orchestrator that ties an [`Ingestor`] and a [`Worker`] together.
///
/// `Ingestix` owns:
/// - a bounded queue (`buffer_size`) between ingestor and workers,
/// - a concurrency semaphore (`concurrency`),
/// - failure tracking based on [`WorkerFailurePolicy`],
/// - optional `/metrics` and `/health/*` endpoints (when `metrics` is enabled).
pub struct Ingestix<M, C, S> {
    context: Arc<SharedContext<C, S>>,
    buffer_size: usize,
    worker_failure_policy: WorkerFailurePolicy,
    worker_failures: Arc<AtomicUsize>,
    readiness_failure_threshold: Option<usize>,
    _marker: std::marker::PhantomData<M>,
}

impl<M, C, S> Ingestix<M, C, S>
where
    M: Send + 'static,
    C: Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    /// Create a new pipeline orchestrator.
    ///
    /// `concurrency` is the maximum number of worker tasks that can process messages
    /// at the same time. `buffer_size` bounds the internal queue between ingestion and processing.
    pub fn new(config: C, state: S, concurrency: usize, buffer_size: usize) -> Self {
        Self {
            context: Arc::new(SharedContext {
                config: ArcSwap::from_pointee(config),
                state,
                semaphore: Arc::new(Semaphore::new(concurrency)),
            }),
            buffer_size,
            worker_failure_policy: WorkerFailurePolicy::BestEffort,
            worker_failures: Arc::new(AtomicUsize::new(0)),
            readiness_failure_threshold: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Switch the failure strategy for worker processing.
    pub fn with_worker_failure_policy(mut self, policy: WorkerFailurePolicy) -> Self {
        self.worker_failure_policy = policy;
        self
    }

    /// Enable readiness failure if too many worker failures happen.
    ///
    /// If set to `threshold`, readiness will return `503 Service Unavailable` once
    /// the number of recorded worker failures reaches `threshold`.
    pub fn with_readiness_failure_threshold(mut self, threshold: usize) -> Self {
        self.readiness_failure_threshold = Some(threshold.max(1));
        self
    }

    #[cfg(feature = "metrics")]
    /// Start a monitor server on `127.0.0.1:<port>`.
    ///
    /// Exposes:
    /// - `GET /metrics`
    /// - `GET /health/live`
    /// - `GET /health/ready` (optionally fails when worker failures exceed the configured threshold)
    pub async fn spawn_monitor_server(&self, port: u16) -> anyhow::Result<()> {
        // Bind to loopback by default to avoid accidentally exposing metrics/health publicly.
        self.spawn_monitor_server_on(std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
        .await
    }

    #[cfg(feature = "metrics")]
    /// Start a monitor server on a specific address.
    pub async fn spawn_monitor_server_on(&self, addr: std::net::SocketAddr) -> anyhow::Result<()> {
        let worker_failures = self.worker_failures.clone();
        let readiness_failure_threshold = self.readiness_failure_threshold;
        let app = Router::new()
            .route("/metrics", get(handle_metrics))
            .route("/health/live", get(|| async { StatusCode::OK }))
            .route(
                "/health/ready",
                get(move || {
                    let worker_failures = worker_failures.clone();
                    async move {
                        if let Some(threshold) = readiness_failure_threshold
                            && worker_failures.load(Ordering::Relaxed) >= threshold
                        {
                            return StatusCode::SERVICE_UNAVAILABLE;
                        }
                        StatusCode::OK
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind(addr).await?;
        tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app).await {
                crate::flow_log_warn!("Monitor server exited with error: {}", err);
            }
        });
        Ok(())
    }

    /// Run the pipeline until shutdown.
    ///
    /// This method:
    /// 1. spawns the ingestor,
    /// 2. spawns worker tasks as messages arrive (bounded by a semaphore),
    /// 3. waits for `Ctrl-C` or `SIGTERM`,
    /// 4. then drains outstanding worker tasks and returns the first error
    ///    (unless `BestEffort` is selected).
    pub async fn launch<I, W>(self, ingestor: I, worker: W) -> anyhow::Result<()>
    where
        I: Ingestor<M, C, S>,
        W: Worker<M, C, S>,
    {
        #[cfg(feature = "metrics")]
        let _ = maybe_prom_handle();

        let (tx, mut rx) = mpsc::channel(self.buffer_size);
        let (shutdown_tx, _) = broadcast::channel(1);
        let worker = Arc::new(worker);
        let mut worker_tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();
        let worker_failure_policy = self.worker_failure_policy;
        let worker_failures = self.worker_failures.clone();

        let i_ctx = self.context.clone();
        let i_shutdown = shutdown_tx.subscribe();
        let ingestor_handle =
            tokio::spawn(async move { ingestor.run(tx, i_ctx, i_shutdown).await });

        let signal_shutdown_tx = shutdown_tx.clone();
        let signal_task = tokio::spawn(async move {
            let signal_name = wait_for_shutdown_signal().await;
            crate::flow_log_info!(
                "Ingestix received {} signal, broadcasting shutdown",
                signal_name
            );
            let _ = signal_shutdown_tx.send(());
        });

        while let Some(msg) = rx.recv().await {
            CURRENT_QUEUE.set(rx.len() as i64);
            let w = worker.clone();
            let c = self.context.clone();
            let worker_shutdown_tx = shutdown_tx.clone();
            let worker_failures_for_task = worker_failures.clone();
            let permit = self
                .context
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| anyhow::anyhow!("worker semaphore unexpectedly closed: {e}"))?;

            worker_tasks.spawn(async move {
                let _permit = permit;
                match w.process(msg, c).await {
                    Ok(()) => {
                        PROCESSED_MSGS.inc();
                        Ok(())
                    }
                    Err(err) => {
                        worker_failures_for_task.fetch_add(1, Ordering::Relaxed);
                        crate::flow_log_warn!("Worker failed to process message: {}", err);
                        if matches!(worker_failure_policy, WorkerFailurePolicy::FailFast) {
                            let _ = worker_shutdown_tx.send(());
                        }
                        Err(err)
                    }
                }
            });
        }

        signal_task.abort();
        let ingestor_result = ingestor_handle
            .await
            .map_err(|e| anyhow::anyhow!("ingestor task failed to join: {e}"))?;
        ingestor_result?;

        let mut first_worker_error: Option<anyhow::Error> = None;
        while let Some(result) = worker_tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if first_worker_error.is_none() {
                        first_worker_error = Some(err);
                    }
                }
                Err(err) => {
                    self.worker_failures.fetch_add(1, Ordering::Relaxed);
                    let join_err = anyhow::anyhow!("worker task join error: {err}");
                    crate::flow_log_warn!("{}", join_err);
                    if first_worker_error.is_none() {
                        first_worker_error = Some(join_err);
                    }
                }
            }
        }

        if matches!(worker_failure_policy, WorkerFailurePolicy::FailFast)
            && let Some(err) = first_worker_error
        {
            return Err(err);
        }

        Ok(())
    }
}

async fn wait_for_shutdown_signal() -> &'static str {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => "SIGINT",
        _ = wait_for_sigterm() => "SIGTERM",
    }
}

#[cfg(unix)]
async fn wait_for_sigterm() {
    use tokio::signal::unix::{SignalKind, signal};

    if let Ok(mut term_signal) = signal(SignalKind::terminate()) {
        let _ = term_signal.recv().await;
    } else {
        std::future::pending::<()>().await;
    }
}

#[cfg(not(unix))]
async fn wait_for_sigterm() {
    std::future::pending::<()>().await;
}

#[cfg(feature = "metrics")]
async fn handle_metrics() -> String {
    if let Some(handle) = maybe_prom_handle() {
        handle.run_upkeep();
        handle.render()
    } else {
        "# metrics recorder unavailable\n".to_string()
    }
}

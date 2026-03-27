# Ingestix follow-up todo (post v0.1 hardening)

This is the short list of remaining security/functional items that should be addressed after the current hardening pass.

## Recently completed (v0.1 hardening)

- [x] Added HTTP queue/backpressure policy (`Block`, `Reject503`, `Reject429`) with explicit overload responses.
- [x] Switched API-key/token checks to constant-time comparison.
- [x] Added configurable TCP hardening knobs (`max_connections`, `read_idle_timeout`, `max_frame_length`).
- [x] Added HTTP body-size limit and request-timeout configuration.
- [x] Added rejection metrics for invalid JSON, invalid API key, oversized payload, timeout, and overloaded queue.

## Reliability and observability improvements

- [x] Added tests for `WorkerFailurePolicy::FailFast` (worker error + panic) to verify shutdown/error contract.
- [x] Clarified metrics semantics (`received` vs `enqueued` vs `processed`) in docs and expanded race-focused shutdown/reliability tests.
- [x] Added optional readiness degradation based on worker-failure threshold.
- [x] Added HTTP auth behavior matrix tests (`x-api-key`, `Bearer`, `Basic`, missing/malformed headers).

## Nice to have

- [ ] Add optional TLS guidance/examples (or clear reverse-proxy recommendations) for HTTP/TCP ingestors in production deployments.
- [x] Published release checklist in `RELEASE_CHECKLIST.md` for runtime contracts, shutdown behavior, and load/backpressure expectations.

## Next release (security + abuse-resistance)
- [ ] Ensure `HttpConfig::request_timeout` cannot be bypassed by CPU-heavy JSON parsing (offload `serde_json::from_slice` to `spawn_blocking` or similar, and add regression tests).
- [ ] Add optional HTTP request rate limiting (and/or fair overload controls) to complement existing queue backpressure (`HttpQueuePolicy`).
- [ ] Add security headers (e.g. `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, cache control) to monitor endpoints (`/metrics`, `/health/*`) similarly to ingestor responses.

# Ingestix Release Checklist

Use this checklist before cutting any new release (`0.x.y`).

## 1) Build and quality gates

- [ ] `cargo test -p ingestix --all-features` passes.
- [ ] `cargo clippy -p ingestix --all-features -- -D warnings` passes.
- [ ] `cargo checkall` passes (workspace/examples validation).

## 2) Runtime contract verification

- [ ] Confirm shutdown behavior contract still holds:
  - ingestor stops accepting new work first
  - queued messages are drained
  - worker tasks complete before process exit
- [ ] Confirm worker failure policy behavior is unchanged or intentionally changed:
  - `BestEffort` vs `FailFast`
  - update docs/changelog if default/semantics changed
- [ ] Confirm readiness degradation behavior (if configured via `with_readiness_failure_threshold`) is documented and tested.

## 3) Ingestor/backpressure behavior

- [ ] HTTP queue policy is documented (`Block`, `Reject503`, `Reject429`).
- [ ] HTTP defaults are documented and sensible:
  - `max_body_bytes`
  - `request_timeout`
  - `enqueue_timeout`
- [ ] TCP defaults are documented and sensible:
  - `max_connections`
  - `read_idle_timeout`
  - `max_frame_length`
- [ ] UDP limits/defaults are documented and sensible:
  - `max_datagram_size`

## 4) Metrics and observability contract

- [ ] Metric meanings are accurate in docs:
  - `flow_received_total` = valid parsed payloads before enqueue attempt
  - `flow_ingested_total` = successfully enqueued payloads
  - `flow_processed_total` = successfully processed worker messages
  - `flow_rejected_*_total` = categorized rejects
- [ ] `/health/live` and `/health/ready` behavior is documented and unchanged.
- [ ] Rejection paths are covered by tests for auth/invalid payload/overload/shutdown edge cases.

## 5) Security and deployment assumptions

- [ ] API key handling remains redacted in debug/log output.
- [ ] Constant-time secret comparison is intact for auth checks.
- [ ] TLS deployment assumption is documented:
  - TLS is terminated by reverse proxy (e.g. Nginx/Traefik) for now.
- [ ] Any newly introduced limits/timeouts are documented as security hardening controls.

## 6) Docs, versioning, and release hygiene

- [ ] README/examples reflect current APIs and defaults (no stale struct literals/config fields).
- [ ] Changelog/release notes include:
  - behavior changes
  - new defaults
  - any migration notes for users
- [ ] Version bump is correct and consistent in workspace/crates.
- [ ] Git tag/release artifact names match the version.

## 7) Final smoke checks (recommended)

- [ ] Run one HTTP example and verify:
  - valid requests return `202`
  - overload behavior matches configured queue policy
  - shutdown returns `503` during the shutdown window
- [ ] Run TCP/UDP examples with sample payloads and confirm clean shutdown.

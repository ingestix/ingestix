# http_webhook

HTTP webhook example with a strongly typed request body.

## What it shows

- How to ingest JSON over HTTP using `HttpIngestor`.
- How to deserialize directly into a Rust struct (`WebhookPayload`).
- How to process typed messages with a `FlowWorker`.
- Uses the `ingestix` crate with explicit features: `derive` + `ingestors`.

## Run

From repository root:

```bash
cargo run -p http_webhook
```

The example starts:

- Ingest endpoint: `http://localhost:3000/ingest`
- Monitor endpoints: `http://localhost:8080/metrics`, `http://localhost:8080/health/live`, `http://localhost:8080/health/ready`

## Send test data

```bash
curl -X POST http://localhost:3000/ingest \
  -H "content-type: application/json" \
  -d '{"event":"user.created","data":"abc123"}'
```

You should see the parsed payload in the worker logs.

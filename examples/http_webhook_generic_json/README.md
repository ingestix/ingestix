# http_webhook_generic_json

HTTP webhook example that accepts unknown JSON shape using `serde_json::Value`.

## What it shows

- How to reuse `HttpIngestor` without a predefined payload struct.
- How to accept dynamic JSON bodies as `serde_json::Value`.
- How to inspect or route generic JSON in your worker logic.
- Uses the `ingestix` crate with explicit features: `derive` + `ingestors`.

## Run

From repository root:

```bash
cargo run -p http_webhook_generic_json
```

The example starts:

- Ingest endpoint: `http://localhost:3000/ingest`
- Monitor endpoints: `http://localhost:8080/metrics`, `http://localhost:8080/health/live`, `http://localhost:8080/health/ready`

## Send test data

Any valid JSON value is accepted:

```bash
curl -X POST http://localhost:3000/ingest \
  -H "content-type: application/json" \
  -d '{"event":"x","data":{"id":1,"tags":["a","b"]},"ok":true}'
```

```bash
curl -X POST http://localhost:3000/ingest \
  -H "content-type: application/json" \
  -d '{"arbitrary":{"nested":{"payload":"works"}}}'
```

You should see the raw JSON value in worker logs.

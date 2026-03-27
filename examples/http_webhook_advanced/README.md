# http_webhook_advanced

HTTP webhook example with generic JSON payload plus request-header validation.

## What it shows

- How to ingest unknown JSON shape as `serde_json::Value`.
- How to enforce `Content-Type: application/json` in the ingestor.
- How to validate an API key header (`x-api-key`) before worker processing.
- Uses the `ingestix` crate with explicit features: `derive` + `ingestors`.

## Run

From repository root:

```bash
cargo run -p http_webhook_advanced
```

The example starts:

- Ingest endpoint: `http://localhost:3000/ingest`
- Monitor endpoints: `http://localhost:8080/metrics`, `http://localhost:8080/health/live`, `http://localhost:8080/health/ready`

## Send test data

For HTTP auth, set `INGESTIX_API_KEY`:

```bash
export INGESTIX_API_KEY='demo-secret'
```

Send a valid request (correct content type + API key):

```bash
curl -X POST http://localhost:3000/ingest \
  -H "content-type: application/json" \
  -H "x-api-key: $INGESTIX_API_KEY" \
  -d '{"event":"x","data":{"id":1,"tags":["a","b"]},"ok":true}'
```

Wrong content type (returns `415` + JSON error):

```bash
curl -X POST http://localhost:3000/ingest \
  -H "content-type: text/plain" \
  -H "x-api-key: $INGESTIX_API_KEY" \
  -d '{"event":"wrong-type"}'
```

Missing/wrong API key (returns `401` + JSON error):

```bash
curl -X POST http://localhost:3000/ingest \
  -H "content-type: application/json" \
  -d '{"event":"missing-key"}'
```

You should see only accepted requests in worker logs.

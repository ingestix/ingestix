# udp_basic

UDP ingestion example with a strongly typed JSON payload.

## What it shows

- How to ingest JSON datagrams over UDP using `UdpIngestor`.
- How to deserialize directly into a Rust struct (`MyMsg`).
- How to process typed messages with a `FlowWorker`.
- Uses the `ingestix` crate with explicit features: `derive` + `ingestors` + `metrics` + `logging`.

## Run

From repository root:

```bash
cargo run -p udp_basic
```

The example starts:

- UDP ingest socket: `0.0.0.0:9000`
- Monitor endpoints: `http://localhost:8080/metrics`, `http://localhost:8080/health/live`, `http://localhost:8080/health/ready`

## Send test data

Using `netcat`:

```bash
echo '{"id":"sensor-1","val":42.5}' | nc -u -w1 localhost 9000
```

You should see the parsed payload printed by the worker.

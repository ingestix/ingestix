# syslog_basic

Syslog ingestion example using `SyslogIngestor` (UDP) and a strongly typed `SyslogEvent`.

## What it shows

- How to ingest syslog messages over UDP using `SyslogIngestor`.
- How to parse RFC3164/RFC5424 syslog lines into `SyslogEvent`.
- How to process events with a `FlowWorker`.

## Run

From repository root:

```bash
cargo run -p syslog_basic
```

The example starts:

- Ingest endpoint: `udp://localhost:9000`
- Monitor endpoints: `http://localhost:8080/metrics`, `http://localhost:8080/health/live`, `http://localhost:8080/health/ready`

## Send test data

Using `nc`:

```bash
echo '<34>Oct 11 22:14:15 mymachine su: hello rfc3164' | nc -u -w1 localhost 9000
```

Example RFC5424:

```bash
echo '<34>1 2026-03-30T22:14:15Z mymachine su 1234 ID47 - hello rfc5424' | nc -u -w1 localhost 9000
```


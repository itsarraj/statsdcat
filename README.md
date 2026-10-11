# statsdcat

A fast, lightweight, standalone StatsD & DogStatsD metrics inspector, listener, rolling aggregator, and synthetic emitter.

`statsdcat` gives backend and DevOps developers an instant, zero-configuration CLI tool for debugging StatsD telemetry. Instead of setting up heavy Docker containers (Datadog Agent, Telegraf, statsd_exporter) or wrestling with raw `nc -ul 8125` scrolling garble, `statsdcat` parses, colorizes, filters, and computes rolling percentiles and throughput directly in your terminal.

---

## Status

**Status: Complete & Verified**
- Verified live ingestion of multi-metric newline-delimited UDP datagrams against real network sockets.
- Full metric type coverage: Counters (`|c`), Gauges (`|g`), Timers (`|ms`), Histograms (`|h`), Distributions (`|d`), Sets (`|s`), Sample Rates (`@0.1`), and DogStatsD tags (`|#env:prod,cluster:us-east`).
- Rolling aggregation engine verified: rate/s, counts, gauge min/max/last, set unique cardinality, and timer percentiles (min, p50, p95, p99, max).
- Synthetic emitter tested for packet generation and rate pacing.
- *Deferred*: Metric forwarding/proxying to remote collectors is planned for a subsequent release.

---

## Features

- **Zero Heavy Daemons**: Standalone binary that binds directly to UDP port `8125` (or custom address).
- **DogStatsD First-Class Support**: Parses `#tag:value` pairs, sample rates (`@0.5`), and multi-line datagram payloads.
- **Two Operating Modes**:
  - **Live Stream Mode** (default): Prints each metric line with colored badges (`[CTR]`, `[GAG]`, `[TMR]`, `[SET]`), source IP, and tag highlights.
  - **Aggregated Terminal Dashboard** (`--aggregate 1s` / `-a`): Computes rolling rates, throughput, and latency percentiles (min, p50, p95, p99, max) in a clean ASCII table.
- **Precision Filtering**:
  - Filter by metric name regex (`--metric "^api\..*"`)
  - Filter by metric type (`--type timer` / `-t counter`)
  - Filter by DogStatsD tags (`--tag "env:production"` or `--tag "region"`)
- **Structured NDJSON Streaming**: Single-line JSON mode (`-j` / `--ndjson`) for streaming directly into `jq`, ClickHouse, or log forwarders.
- **Synthetic Emitter (`statsdcat emit`)**: Generate single or high-frequency test metrics (`--rate <pps>`) to verify your telemetry pipelines without writing ad-hoc scripts.

---

## Installation

```bash
cargo install --path .
```

---

## Usage

### 1. Live Streaming & Inspection

Listen on standard UDP port `8125`:
```bash
statsdcat
```

Listen on a custom port without color:
```bash
statsdcat listen -b 127.0.0.1:9125 --no-color
```

Filter by metric name regex:
```bash
statsdcat -m "^checkout\..*"
```

Filter by DogStatsD tag and metric type:
```bash
statsdcat --tag "env:prod" --type timer
```

Stream as NDJSON and pipe to `jq`:
```bash
statsdcat -j | jq '.name + ": " + (.value_f64 | tostring)'
```

Exit after receiving 500 metrics:
```bash
statsdcat -n 500
```

### 2. Real-Time Rolling Aggregation

Display a 1-second rolling aggregation table of metric rates, percentiles, and counts:
```bash
statsdcat -a 1.0
```

Group aggregations by distinct tag combinations:
```bash
statsdcat -a 2.0 --group-by-tags
```

Output aggregated windows as NDJSON:
```bash
statsdcat -a 5.0 -j
```

### 3. Emitting Synthetic Metrics

Send a single counter metric:
```bash
statsdcat emit -t 127.0.0.1:8125 -m "users.login" -v 1 --type counter --tag "env:dev"
```

Send 100 latency timer metrics at 20 packets/sec:
```bash
statsdcat emit -t 127.0.0.1:8125 -m "db.query_latency" -v 42.5 --type timer --tag "db:postgres" -n 100 --rate 20
```

---

## CLI Reference

### `statsdcat listen`
| Flag | Description | Default |
|---|---|---|
| `-b, --bind <ADDR>` | UDP listener address | `127.0.0.1:8125` |
| `-a, --aggregate <SEC>`| Rolling aggregation window duration | Live stream mode |
| `--group-by-tags` | Group aggregated metrics by tag combinations | False |
| `-j, --ndjson` | Emit single-line JSON | False |
| `-m, --metric <REGEX>` | Regex pattern to filter metric names | None |
| `-t, --type <TYPE>` | Filter by metric type (`counter`, `gauge`, `timer`, etc.) | None |
| `--tag <KEY[:VAL]>` | Filter by tag key or `key:val` | None |
| `-n, --count <N>` | Exit after receiving N metrics | Unlimited |
| `--no-color` | Disable ANSI color highlighting | Color enabled |

### `statsdcat emit`
| Flag | Description | Default |
|---|---|---|
| `-t, --target <ADDR>` | Destination UDP socket address | `127.0.0.1:8125` |
| `-m, --metric <NAME>` | Metric name | Required |
| `-v, --value <VAL>` | Metric value (string or float) | `1` |
| `-y, --type <TYPE>` | Metric type (`counter`, `gauge`, `timer`, `set`, etc.) | `counter` |
| `--sample-rate <R>` | Sample rate float (e.g. `0.1`) | `1.0` |
| `--tag <TAG>` | DogStatsD tags (repeatable) | None |
| `-n, --count <N>` | Number of packets to transmit | `1` |
| `--rate <PPS>` | Packets per second rate limit (0 = max speed) | `0` |

---

## License

MIT License. Copyright (c) 2026 Animesh Raj.

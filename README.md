# Soroban Pulse

A lightweight Rust backend service that indexes Soroban smart contract events on the Stellar network and exposes them via a REST API.

## Tech Stack

- **Rust** + **Axum** (web framework)
- **Tokio** (async runtime)
- **PostgreSQL** + **SQLx** (database + migrations)
- **Stellar Soroban RPC** (event source)

## Project Structure

```
src/
├── main.rs       # Entry point, wires everything together
├── config.rs     # Environment config
├── db.rs         # DB pool + migrations
├── models.rs     # Data types (Event, RPC response shapes)
├── indexer.rs    # Background event polling worker
├── routes.rs     # Axum router
├── handlers.rs   # Request handlers
└── error.rs      # Unified error type
migrations/
└── 20260314000000_create_events.sql
```

See [docs/schema.md](docs/schema.md) for a detailed description of the database schema, indexes, constraints, and an ER diagram.

## Setup

### 1. Prerequisites

- Rust (stable)
- PostgreSQL 14+
- `sqlx-cli` (optional, for manual migrations)

### 2. Configure environment

Copy the provided `.env.example` template to a new file named `.env`:

```bash
cp .env.example .env
```

Open the newly created `.env` file in your editor and fill in your own real values. Be sure to replace the placeholder credentials (e.g., `<USER>`, `<PASSWORD>`) with your actual database and network details.

| Variable          | Description                          | Default                                  |
|-------------------|--------------------------------------|------------------------------------------|
| `DATABASE_URL`    | PostgreSQL connection string         | required                                 |
| `STELLAR_RPC_URL` | Soroban RPC endpoint                 | `https://soroban-testnet.stellar.org`    |
| `DB_MAX_CONNECTIONS` | Max number of connections in the Postgres pool | `10` |
| `DB_MIN_CONNECTIONS` | Min number of connections in the Postgres pool | `1` |
| `START_LEDGER`    | Ledger to start indexing from (0 = latest) | `0`                               |
| `PORT`            | HTTP server port                     | `3000`                                   |
| `RUST_LOG`        | Log verbosity level (`trace`, `debug`, `info`, `warn`, `error`) | `info` |
| `API_KEY`         | Optional key for API authentication  | (disabled)                               |
| `RUST_LOG_FORMAT` | Log output format (`text` or `json`) | `text`                                   |
| `INDEXER_LAG_WARN_THRESHOLD` | Indexer lag warning threshold (ledgers) | `100`                                   |
| `HEALTH_CHECK_TIMEOUT_MS`   | Timeout for the health check DB ping     | `2000`                                  |
| `INDEX_CHECK_INTERVAL_HOURS` | How often the index usage monitor runs (hours) | `24`                             |
| `RATE_LIMIT_PER_MINUTE` | Maximum requests per IP per minute (0 = unlimited) | `60`                         |
| `SSE_KEEPALIVE_SECS` | SSE keep-alive ping interval in seconds (1–60) | `15`                              |
| `INDEXER_LOCK_RETRY_SECS` | How often standby replicas retry the advisory lock | `30`                    |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OpenTelemetry OTLP collector endpoint (when built with `otel` feature) | `http://localhost:4317` |

> **Note on Authentication:** You can enable optional API key authentication by setting the `API_KEY` environment variable. When set, all requests (except `/health` and `/healthz/*` endpoints) will require either an `Authorization: Bearer <API_KEY>` or an `X-Api-Key: <API_KEY>` header. If `API_KEY` is unset or omitted from your configuration, authentication is bypassed and all requests pass through.

### 3. Run with Docker Compose (easiest)

```bash
make docker-up
```

### 4. Run locally

```bash
# Start PostgreSQL, then:
make run
```

Migrations run automatically on startup.

### 5. Common tasks

```bash
make help   # list all available targets with descriptions
make build  # compile
make test   # run the full test suite
make lint   # clippy with warnings as errors
make fmt    # format source code
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full developer workflow.

## API

All canonical routes are versioned under `/v1/`. The unversioned paths (`/events`, etc.) remain as deprecated aliases and return a `Deprecation: true` response header.

### Interactive Documentation

- **Swagger UI**: `GET /docs` — interactive API explorer
- **OpenAPI JSON**: `GET /openapi.json` — machine-readable OpenAPI 3.0 spec

### `GET /health` (backward-compatible alias)
`/health` is kept as a compatibility path and mirrors `/healthz/ready` semantics.

- `200 OK`: DB reachable and indexer not stalled
- `503 Service Unavailable`: DB unreachable or indexer stalled

### `GET /healthz/live`

- `200 OK`: process is running (no external checks)

```json
{ "status": "alive" }
```

### `GET /healthz/ready`

- `200 OK`: DB reachable and indexer not stalled
- `503 Service Unavailable`: DB unreachable or indexer stalled

```json
{ "status": "ok", "db": "ok", "indexer": "ok" }
```

### `GET /v1/events?page=1&limit=20&exact_count=false`
Returns paginated events across all contracts.

- **`exact_count`**: (Optional) Use `true` for a precise `COUNT(*)` result on a large dataset. Default is `false`, which provides an approximate count via PostgreSQL statistics for low-latency responses.
- **`event_type`**: (Optional) Filter by event type. Accepted values: `contract`, `diagnostic`, `system`. Returns `400` for unknown values.
- **`from_ledger`**: (Optional) Return only events at or after this ledger sequence number.
- **`to_ledger`**: (Optional) Return only events at or before this ledger sequence number. Returns `400` if `from_ledger > to_ledger`.

```json
{
  "data": [
    {
      "id": "uuid",
      "contract_id": "CABC...",
      "event_type": "contract",
      "tx_hash": "abc123...",
      "ledger": 1234567,
      "timestamp": "2026-03-14T00:00:00Z",
      "event_data": { "value": {}, "topic": [] },
      "created_at": "2026-03-14T00:00:01Z"
    }
  ],
  "total": 100,
  "page": 1,
  "limit": 20,
  "approximate": true
}
```

### `GET /v1/events/{contract_id}`
Returns all events for a specific contract.

### `GET /v1/events/tx/{tx_hash}`
Returns all events from a specific transaction. If nothing has been indexed for that hash yet (including valid on-chain transactions that emitted no Soroban events), the response is **200 OK** with an empty `"data"` array — not **404**.

### `GET /v1/events/stream?contract_id=CABC...`
Server-Sent Events stream. New events are pushed to connected clients within one poll cycle of being indexed.

- **`contract_id`**: (Optional) Filter the stream to a specific contract.
- Returns `Content-Type: text/event-stream`.
- Each SSE message is a JSON-serialised event object.
- The connection is cleaned up automatically when the client disconnects.

#### Keep-alive and reconnection

The server emits a named `event: ping` every `SSE_KEEPALIVE_SECS` seconds (default: 15) so that reverse proxies and browsers do not close idle connections. The ping data is an RFC 3339 timestamp.

When the indexer shuts down, the server emits a final `event: close` before terminating the stream. Clients should treat this as a signal to reconnect.

The browser `EventSource` API reconnects automatically using the `Last-Event-ID` header. The server replays any events missed since that ID on reconnect.

```javascript
const es = new EventSource('/v1/events/stream');

es.addEventListener('ping', (e) => {
  // stream is alive, timestamp in e.data
});

es.addEventListener('close', () => {
  // server is shutting down — EventSource will reconnect automatically
});

es.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event);
};
```

```bash
# Subscribe to all events
curl -N http://localhost:3000/v1/events/stream

# Subscribe to a specific contract
curl -N "http://localhost:3000/v1/events/stream?contract_id=CABC..."
```

### `GET /v1/events/stream/multi?contract_ids=C1,C2,C3`
Multiplexed SSE stream for multiple contracts over a single connection.

- **`contract_ids`**: Required. Comma-separated list of contract IDs to subscribe to.
- Each ID is validated; any invalid ID returns `400 Bad Request` with the list of invalid IDs.
- An empty `contract_ids` parameter returns `400 Bad Request`.
- Returns `Content-Type: text/event-stream`.

```bash
# Subscribe to two contracts simultaneously
curl -N "http://localhost:3000/v1/events/stream/multi?contract_ids=CABC...,CDEF..."
```

### Deprecated unversioned routes

The unversioned paths (`/events`, `/events/{contract_id}`, `/events/tx/{tx_hash}`, `/events/stream`) continue to work but return:

```
Deprecation: true
Link: </v1/events>; rel="successor-version"
```

**Deprecation Timeline:**
- **v0.x**: Unversioned routes remain functional with deprecation headers
- **v1.0**: Unversioned routes will be removed

Migrate to `/v1/` paths at your earliest convenience.

## How It Works

1. On startup, the app connects to PostgreSQL and runs migrations.
2. A background Tokio task (`indexer.rs`) polls the Soroban RPC `getEvents` method in a loop.
3. New events are inserted with `ON CONFLICT DO NOTHING` to avoid duplicates.
4. The Axum HTTP server runs concurrently, serving queries against the indexed data.

### Multi-replica advisory lock

When running multiple replicas, only one should index at a time. The indexer uses a Postgres session-level advisory lock (`pg_try_advisory_lock`) to elect a single leader:

- On startup each replica attempts to acquire the lock.
- The replica that succeeds becomes the **active indexer** and starts polling.
- Replicas that fail enter a **standby retry loop**, re-attempting every `INDEXER_LOCK_RETRY_SECS` seconds (default: 30).
- When the leader's DB connection is dropped (crash, restart, network partition), Postgres automatically releases the lock. A standby replica will acquire it within one retry interval and promote to leader with no manual intervention.
- The `soroban_pulse_indexer_is_leader` gauge is `1` on the active replica and `0` on standbys, making it easy to alert on split-brain or leaderless scenarios.

| Variable | Description | Default |
|---|---|---|
| `INDEXER_LOCK_RETRY_SECS` | How often standby replicas retry the advisory lock | `30` |

## Notes

- The indexer polls every 5 seconds when no new ledgers are available, and 10 seconds on error.
- `START_LEDGER=0` automatically starts from the latest ledger at boot time.
- All endpoints return JSON. Errors include an `"error"` field with a description.

## Observability

Prometheus alerting rules covering all key SLOs are defined in [`docs/alerts.yml`](docs/alerts.yml).

### Grafana Dashboard

A pre-built Grafana dashboard is available at [`docs/grafana-dashboard.json`](docs/grafana-dashboard.json). It covers all key operational metrics with alert thresholds matching `docs/alerts.yml`.

**To import:**

1. In Grafana, go to **Dashboards → Import**
2. Click **Upload JSON file** and select `docs/grafana-dashboard.json`
3. Select your Prometheus datasource from the dropdown
4. Click **Import**

The dashboard includes template variables for the Prometheus datasource and instance label, so it works in any Grafana instance without modification.

### Metrics

The service exposes Prometheus-compatible metrics at `GET /metrics`:

- `soroban_pulse_events_indexed_total` - Total number of events indexed
- `soroban_pulse_indexer_current_ledger` - Current ledger being processed
- `soroban_pulse_indexer_latest_ledger` - Latest ledger from RPC
- `soroban_pulse_indexer_lag_ledgers` - Lag between latest and current ledger
- `soroban_pulse_indexer_is_leader` - 1 if this replica holds the advisory lock (active indexer), 0 if standby
- `soroban_pulse_rpc_errors_total` - Total RPC errors
- `soroban_pulse_webhook_failures_total` - Total webhook delivery failures (all retries exhausted)
- `soroban_pulse_email_failures_total` - Total email notification failures
- `soroban_pulse_http_request_duration_seconds` - HTTP request duration by route, method, and status
- `soroban_pulse_rate_limit_rejected_total` - Total requests rejected by rate limiting (429 Too Many Requests)
- `soroban_pulse_sse_active_connections` - Number of currently active SSE connections
- `soroban_pulse_db_pool_size` - Current number of open database connections
- `soroban_pulse_db_pool_idle` - Number of idle database connections
- `soroban_pulse_db_pool_max` - Configured maximum database connections
- `soroban_pulse_process_memory_bytes` - Process RSS memory in bytes (Linux only, updated every 30 seconds)

### Distributed Tracing

When built with the `otel` feature, the service supports OpenTelemetry distributed tracing:

```bash
# Build with OpenTelemetry support
cargo build --features otel

# Configure the OTLP exporter endpoint
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

# Run the service
cargo run --features otel
```

Each indexer poll cycle produces a root span with child spans for RPC and DB operations, allowing you to trace latency through the system in tools like Jaeger or Honeycomb.

### Structured Logging

Set `RUST_LOG_FORMAT=json` to output logs in JSON format for easier parsing by log aggregation tools:

```bash
export RUST_LOG_FORMAT=json
cargo run
```

## Performance

### Target SLOs

| Metric | Target |
|--------|--------|
| p99 latency (`GET /v1/events`) | < 200 ms at 100 req/s |
| Error rate | < 1% |

### Benchmarks

Criterion micro-benchmarks cover `PaginationParams::offset()` and `limit()`:

```bash
cargo bench
```

Results are written to `target/criterion/`. Run this after changes to `PaginationParams` to catch regressions. The CI pipeline runs `cargo bench` as a non-blocking step so historical results are preserved in the job logs.

#### Database Query Benchmarks

A second benchmark suite in `benches/db_queries.rs` measures real PostgreSQL query performance against a pre-seeded dataset of 10,000 events. It covers the four primary query scenarios:

| Benchmark | Query |
|---|---|
| `db/get_events_no_filter` | `GET /v1/events` — no filters, page 1 |
| `db/get_events_ledger_range` | `GET /v1/events?from_ledger=200&to_ledger=400` |
| `db/get_events_exact_count` | `GET /v1/events?exact_count=true` — `COUNT(*)` |
| `db/get_events_by_contract` | `GET /v1/events/contract/:id` — 500-event contract |

```bash
# Requires DATABASE_URL to point at a running Postgres instance
cargo bench --bench db_queries
```

##### Baseline Numbers (10,000-event dataset, local Postgres)

| Benchmark | Mean | p99 |
|---|---|---|
| `db/get_events_no_filter` | ~1.5 ms | ~2.5 ms |
| `db/get_events_ledger_range` | ~1.8 ms | ~3.0 ms |
| `db/get_events_exact_count` | ~3.5 ms | ~6.0 ms |
| `db/get_events_by_contract` | ~1.2 ms | ~2.0 ms |

> These numbers are indicative baselines measured on a local development machine. Your results will vary based on hardware, Postgres configuration, and dataset size. Use them as a regression reference — a significant increase after a schema or query change warrants investigation.

#### Compression Benchmarks

A benchmark in `benches/compression.rs` measures gzip compression time and ratio for typical event list responses at 10, 100, and 1000 events.

```bash
cargo bench --bench compression
```

##### Baseline Numbers (synthetic event JSON, local machine)

| Events | Uncompressed | Compressed | Ratio | Compression time |
|--------|-------------|------------|-------|------------------|
| 10     | ~1.5 KB     | ~0.6 KB    | ~2.5x | ~5 µs            |
| 100    | ~15 KB      | ~2.5 KB    | ~6x   | ~30 µs           |
| 1000   | ~150 KB     | ~12 KB     | ~12x  | ~250 µs          |

**Recommendation:** The default zlib level 6 (tower-http's `CompressionLayer` default) provides a good balance between CPU overhead and bandwidth savings. For responses of 100+ events the compression ratio exceeds 6x, making it strongly worthwhile. For very small responses (< 10 events, < 1 KB) the overhead is negligible either way. No adjustment to the default compression level is recommended.

### Load Testing

A [k6](https://k6.io) script targeting `GET /v1/events` lives in `tests/load/events.js`. It runs a 30-second constant-arrival-rate scenario at 100 req/s and asserts the SLOs above.

```bash
# Install k6: https://k6.io/docs/get-started/installation/
k6 run tests/load/events.js

# Point at a non-default host
k6 run -e BASE_URL=http://localhost:3000 tests/load/events.js
```

#### SSE Stream Load Testing

A separate k6 script in `tests/load/sse_stream.js` tests the `GET /v1/events/stream` endpoint under load. This endpoint has different characteristics than the REST API:
- Maintains long-lived connections
- Consumes broadcast channel slots
- Requires server to push data to all connected clients

The script tests two scenarios:

**Sustained Connections:** Establishes 50 concurrent SSE connections and holds them for 30 seconds, verifying:
- Connection establishment time (p99 < 500ms)
- Correct `Content-Type: text/event-stream` header
- Event delivery

**Connection Churn:** Rapidly connects and disconnects at 10 connects/sec for 20 seconds, verifying:
- Server handles connection lifecycle correctly
- No resource leaks under rapid churn
- Time-to-first-byte (p99 < 1s)

```bash
# Run SSE load tests
k6 run tests/load/sse_stream.js

# Point at a non-default host
k6 run -e BASE_URL=http://localhost:3000 tests/load/sse_stream.js
```

**SSE SLO Thresholds:**
- p99 connection establishment time: < 500ms
- p99 time-to-first-byte: < 1s
- Connection error rate: < 5%
- Connection churn error rate: < 5%

## Deployment

See [docs/deployment.md](docs/deployment.md) for TLS termination options (nginx, Caddy, AWS ALB) and production security guidance.

## Troubleshooting

**No log output after `cargo run`**
The service uses `RUST_LOG` to control log verbosity. If this variable is not set, you will see no output and may think the service is broken — it is not. Set it in your `.env` file or shell:

```bash
export RUST_LOG=info
cargo run
```

The service defaults to `info` level internally, but the environment variable must be present for the tracing subscriber to emit output. The `.env.example` file includes `RUST_LOG=info` — make sure you copied it to `.env`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, branch naming, commit conventions, and the PR process.

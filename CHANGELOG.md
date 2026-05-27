# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Email notification feature for event alerts with batching (one email per minute maximum)
- Email configuration via `EMAIL_SMTP_HOST`, `EMAIL_SMTP_PORT`, `EMAIL_SMTP_USER`, `EMAIL_SMTP_PASSWORD`, `EMAIL_FROM`, `EMAIL_TO`, and `EMAIL_CONTRACT_FILTER` environment variables
- Email notifications can be filtered by contract ID using `EMAIL_CONTRACT_FILTER`
- Prometheus metric `soroban_pulse_email_failures_total` for monitoring email delivery failures
- Documentation for email notifications in `docs/email-notifications.md`
- Contract ID format validation for SSE stream endpoint (`/v1/events/stream`)
- Database pool metrics to Prometheus endpoint (`soroban_pulse_db_pool_size`, `soroban_pulse_db_pool_idle`, `soroban_pulse_db_pool_max`)
- Separate CI job for integration tests with real PostgreSQL
- CHANGELOG.md and release process documentation
- Server-Sent Events (SSE) streaming with keep-alive pings and automatic reconnection support
- OpenTelemetry distributed tracing support (optional `otel` feature)
- Multi-replica advisory lock for leader election and standby failover
- Approximate event count via PostgreSQL statistics for low-latency responses
- Event filtering by type (`contract`, `diagnostic`, `system`)
- Event filtering by ledger range (`from_ledger`, `to_ledger`)
- Lua script transformation for event data (optional `lua` feature)
- Rate limiting per IP address with configurable threshold
- Structured JSON logging support

### Changed
- Removed `--skip handlers::tests` flag from CI test job to run all tests including handler integration tests

### Deprecated
- Unversioned routes (`/events`, `/events/{contract_id}`, `/events/tx/{tx_hash}`, `/events/stream`) will be removed in v2.0.0. Use `/v1/` prefixed routes instead.

### Security
- API key authentication support via `Authorization: Bearer` or `X-Api-Key` headers (optional `API_KEY` environment variable)

## [0.1.0] - 2026-04-21

### Added
- Initial release of Soroban Pulse
- Event indexing from Soroban RPC
- REST API for querying indexed events
- Server-Sent Events (SSE) stream for real-time event notifications
- Prometheus metrics endpoint
- Health check endpoints (`/health`, `/healthz/live`, `/healthz/ready`)
- OpenAPI documentation with Swagger UI
- Database connection pooling with configurable min/max connections
- Rate limiting per IP address
- CORS support
- Structured logging with JSON output option
- OpenTelemetry distributed tracing support (optional feature)
- Docker and Kubernetes deployment configurations
- Comprehensive test suite with integration tests

[Unreleased]: https://github.com/Soroban-Pulse/SorobanPulse/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Soroban-Pulse/SorobanPulse/releases/tag/v0.1.0

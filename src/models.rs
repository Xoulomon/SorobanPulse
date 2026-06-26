use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

/// Notification format for webhooks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type, utoipa::ToSchema)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum NotificationFormat {
    Raw,
    Slack,
    Discord,
    Teams,
    Pagerduty,
}

impl fmt::Display for NotificationFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationFormat::Raw => write!(f, "raw"),
            NotificationFormat::Slack => write!(f, "slack"),
            NotificationFormat::Discord => write!(f, "discord"),
            NotificationFormat::Teams => write!(f, "teams"),
            NotificationFormat::Pagerduty => write!(f, "pagerduty"),
        }
    }
}

impl FromStr for NotificationFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "raw" => Ok(NotificationFormat::Raw),
            "slack" => Ok(NotificationFormat::Slack),
            "discord" => Ok(NotificationFormat::Discord),
            "teams" => Ok(NotificationFormat::Teams),
            "pagerduty" => Ok(NotificationFormat::Pagerduty),
            other => Err(format!("unknown notification format: {other}")),
        }
    }
}

/// Notification priority levels (Issue #492).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum NotificationPriority {
    Critical,
    High,
    Medium,
    Low,
}

impl NotificationPriority {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "critical" => Self::Critical,
            "high" => Self::High,
            "low" => Self::Low,
            _ => Self::Medium,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
        }
    }

    /// Returns true when the notification must be delivered immediately rather than batched.
    pub fn is_immediate(self) -> bool {
        matches!(self, Self::Critical)
    }
}

impl std::fmt::Display for NotificationPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type, utoipa::ToSchema,
)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum EventType {
    Contract,
    Diagnostic,
    System,
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventType::Contract => write!(f, "contract"),
            EventType::Diagnostic => write!(f, "diagnostic"),
            EventType::System => write!(f, "system"),
        }
    }
}

impl FromStr for EventType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "contract" => Ok(EventType::Contract),
            "diagnostic" => Ok(EventType::Diagnostic),
            "system" => Ok(EventType::System),
            other => Err(format!("unknown event type: {other}")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct Event {
    pub id: Uuid,
    pub contract_id: String,
    pub event_type: EventType,
    pub tx_hash: String,
    pub ledger: i64,
    pub timestamp: DateTime<Utc>,
    pub event_data: Value,
    pub event_data_normalized: Option<Value>,
    #[sqlx(default)]
    pub event_data_decoded: Option<Value>,
    #[sqlx(default)]
    pub ledger_hash: Option<String>,
    #[sqlx(default)]
    pub in_successful_call: bool,
    pub created_at: DateTime<Utc>,
    /// Schema version of the Soroban protocol used when this event was indexed.
    #[sqlx(default)]
    pub schema_version: i32,
    /// Whether this event has been anonymized for GDPR compliance.
    #[sqlx(default)]
    pub anonymized: bool,
    #[sqlx(default)]
    #[serde(skip)]
    pub total_count: i64,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct PaginationParams {
    pub page: Option<i64>,
    pub limit: Option<i64>,
    pub exact_count: Option<bool>,
    pub fields: Option<String>,
    pub contract_id: Option<String>,
    /// Comma-separated list of contract IDs to filter by (max 20).
    pub contract_ids: Option<String>,
    pub event_type: Option<EventType>,
    pub from_ledger: Option<i64>,
    pub to_ledger: Option<i64>,
    pub ledger_hash: Option<String>,
    pub cursor: Option<String>,
    pub sort: Option<SortOrder>,
    /// Sort column: `ledger`, `timestamp`, or `created_at` (default: ledger)
    pub sort_by: Option<SortBy>,
    pub in_successful_call: Option<bool>,
    /// Filter by Soroban protocol schema version.
    pub schema_version: Option<i32>,
    /// Filter by anonymized status. Requires an ADMIN_API_KEY.
    pub anonymized: Option<bool>,
    /// Filter by the first topic symbol (uses topic_0_sym generated column index).
    pub topic_sym: Option<String>,
    /// Filter by topic array using JSONB containment (e.g., ?topic=["transfer"]).
    pub topic: Option<String>,
    /// Filter by exact value of topic[0] (e.g. "transfer"). Uses topic_0_sym index.
    pub topic_0: Option<String>,
    /// Filter by exact value of topic[1]. Uses GIN index on event_data->'topic'.
    pub topic_1: Option<String>,
    /// Filter by exact value of topic[2]. Uses GIN index on event_data->'topic'.
    pub topic_2: Option<String>,
    /// Filter by exact value of topic[3]. Uses GIN index on event_data->'topic'.
    pub topic_3: Option<String>,
    /// Full-text search query for event_data (uses event_data_tsv tsvector index).
    pub search: Option<String>,
    /// Filter events at or after this timestamp (ISO 8601 format).
    pub from_timestamp: Option<String>,
    /// Filter events at or before this timestamp (ISO 8601 format).
    pub to_timestamp: Option<String>,
    /// Return event_data as base64-encoded gzip-compressed JSON (default: false).
    pub compact: Option<bool>,
    /// Filter events by contract ID prefix (minimum 4 characters, uses LIKE 'prefix%').
    pub contract_id_prefix: Option<String>,
}

/// Sort order for event list endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    /// Returns the SQL ORDER BY direction string.
    pub fn as_sql(&self) -> &'static str {
        match self {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        }
    }
}

/// Column to sort by for event list endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortBy {
    Ledger,
    Timestamp,
    CreatedAt,
}

impl SortBy {
    /// Returns the SQL column name to sort by.
    pub fn as_sql_col(&self) -> &'static str {
        match self {
            SortBy::Ledger => "ledger",
            SortBy::Timestamp => "timestamp",
            SortBy::CreatedAt => "created_at",
        }
    }
    /// Returns a short string identifier suitable for cursor encoding/decoding.
    pub fn as_tag(&self) -> &'static str {
        match self {
            SortBy::Ledger => "ledger",
            SortBy::Timestamp => "timestamp",
            SortBy::CreatedAt => "created_at",
        }
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct SearchParams {
    pub contract_ids: Option<Vec<String>>,
    pub event_type: Option<EventType>,
    pub from_ledger: Option<i64>,
    pub to_ledger: Option<i64>,
    pub topic_filter: Option<Value>,
    pub page: Option<i64>,
    pub limit: Option<i64>,
}

impl SearchParams {
    pub fn offset(&self) -> i64 {
        let page = self.page.unwrap_or(1).max(1);
        (page - 1) * self.limit()
    }

    pub fn limit(&self) -> i64 {
        self.limit.unwrap_or(20).clamp(1, 100)
    }
}

#[derive(Debug, Deserialize)]
pub struct StreamParams {
    pub contract_id: Option<String>,
    pub fields: Option<String>,
    /// Filter by event type: contract, diagnostic, system
    pub event_type: Option<EventType>,
}

/// Query parameters for the multi-contract SSE stream endpoint.
#[derive(Debug, Deserialize)]
pub struct MultiStreamParams {
    /// Comma-separated list of contract IDs to subscribe to.
    pub contract_ids: Option<String>,
    /// Filter by event type: contract, diagnostic, system
    pub event_type: Option<EventType>,
}

/// Standard error response body returned by all error responses.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    /// Human-readable error description.
    pub error: String,
    /// Machine-readable error code.
    pub code: String,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ExportParams {
    pub event_type: Option<EventType>,
    pub from_ledger: Option<i64>,
    pub to_ledger: Option<i64>,
    pub contract_id: Option<String>,
    /// Output format: "csv" (default), "parquet", or "jsonl"
    pub format: Option<String>,
    /// Optional JSON object mapping source field names to target field names.
    /// Example: `{"event_data":"raw_data","ledger":"ledger_seq"}`
    pub field_map: Option<String>,
    /// Optional ISO 8601 timestamp filter (start)
    pub from_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional ISO 8601 timestamp filter (end)
    pub to_timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

/// Request body for POST /v1/admin/mask-events
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct MaskEventsRequest {
    /// Optional list of contract IDs to mask. If not provided, masks all events.
    pub contract_ids: Option<Vec<String>>,
}

/// Response body for POST /v1/admin/mask-events
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct MaskEventsResponse {
    /// Unique job ID for tracking the masking operation
    pub job_id: String,
    /// Current status: "pending", "running", "completed", or "failed"
    pub status: String,
}

/// Response body for GET /v1/admin/mask-events/:job_id
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct MaskJobStatus {
    /// Unique job ID
    pub job_id: String,
    /// Current status: "pending", "running", "completed", or "failed"
    pub status: String,
    /// Number of events processed so far
    pub processed: i64,
    /// Total number of events to process
    pub total: i64,
    /// Error message if status is "failed"
    pub error: Option<String>,
}

/// Query parameters for GET /v1/events/timeseries
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct TimeseriesParams {
    /// Time bucket: "1h", "1d", "1w", "1mo"
    pub bucket: String,
    /// Optional: filter by contract ID
    pub contract_id: Option<String>,
    /// Optional: start ledger
    pub from_ledger: Option<i64>,
    /// Optional: end ledger
    pub to_ledger: Option<i64>,
}

/// Single time bucket in timeseries response
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct TimeseriesBucket {
    /// Start of the time bucket (ISO 8601)
    pub bucket_start: DateTime<Utc>,
    /// Number of events in this bucket
    pub event_count: i64,
    /// Number of unique contracts in this bucket
    pub contract_count: i64,
    /// Event counts by type
    pub event_types: std::collections::HashMap<String, i64>,
}

/// Response body for GET /v1/events/timeseries
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct TimeseriesResponse {
    /// Time bucket size
    pub bucket: String,
    /// Array of time buckets
    pub data: Vec<TimeseriesBucket>,
}

/// Webhook configuration for formatted notifications
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct WebhookConfig {
    pub id: Uuid,
    pub url: String,
    pub secret: Option<String>,
    pub notification_format: NotificationFormat,
    pub message_template: Option<String>,
    pub contract_filter: Option<Vec<String>>,
    pub event_type_filter: Option<Vec<String>>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// PagerDuty configuration for incident management
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct PagerDutyConfig {
    pub id: Uuid,
    pub routing_key: String,
    pub service_name: String,
    pub contract_filter: Option<Vec<String>>,
    pub event_type_filter: Option<Vec<String>>,
    pub severity_mapping: Value,
    pub auto_resolve: bool,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// PagerDuty incident tracking
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct PagerDutyIncident {
    pub id: Uuid,
    pub dedup_key: String,
    pub contract_id: String,
    pub event_type: String,
    pub incident_key: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Query parameters for GET /v1/events/diff
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct DiffParams {
    pub from_ledger: i64,
    pub to_ledger: i64,
}

/// Per-contract event counts in a diff response.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ContractDiff {
    pub contract_id: String,
    /// Event counts keyed by event type name.
    pub event_counts: std::collections::HashMap<String, i64>,
    /// Total events emitted by this contract in the range.
    pub total: i64,
}

/// Response body for GET /v1/events/diff
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct DiffResponse {
    pub from_ledger: i64,
    pub to_ledger: i64,
    pub contracts: Vec<ContractDiff>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ReplayRequest {
    pub from_ledger: u64,
    pub to_ledger: u64,
}

/// Request body for the batch tx-hash lookup endpoint.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct BatchTxRequest {
    /// List of transaction hashes to look up (max 100).
    pub hashes: Vec<String>,
}

/// Request body for bulk event insertion.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct BulkInsertRequest {
    /// List of events to insert (max 1000 per request).
    pub events: Vec<BulkEventInput>,
}

/// Event input for bulk insertion.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct BulkEventInput {
    pub contract_id: String,
    pub event_type: String,
    pub tx_hash: String,
    pub ledger: i64,
    pub timestamp: DateTime<Utc>,
    pub event_data: Value,
    #[serde(default)]
    pub event_data_normalized: Option<Value>,
    #[serde(default)]
    pub ledger_hash: Option<String>,
    #[serde(default)]
    pub in_successful_call: Option<bool>,
}

/// Response for bulk event insertion.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BulkInsertResponse {
    pub inserted: i64,
    pub skipped: i64,
    pub failed: i64,
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct ContractSummary {
    pub contract_id: String,
    pub event_count: i64,
    pub first_seen_ledger: i64,
    pub last_seen_ledger: i64,
    pub last_event_at: DateTime<Utc>,
}

/// Detailed per-contract summary returned by GET /v1/contracts/:contract_id/summary.
#[derive(Debug, Serialize, Clone, utoipa::ToSchema)]
pub struct ContractDetailSummary {
    pub contract_id: String,
    pub total_events: i64,
    pub first_event_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub unique_tx_count: i64,
    pub ledger_range: LedgerRange,
    pub event_type_breakdown: EventTypeBreakdown,
    /// Whether the data was served from the materialized view (true) or a live query (false).
    pub from_cache: bool,
}

#[derive(Debug, Serialize, Clone, utoipa::ToSchema)]
pub struct LedgerRange {
    pub min: Option<i64>,
    pub max: Option<i64>,
}

#[derive(Debug, Serialize, Clone, utoipa::ToSchema)]
pub struct EventTypeBreakdown {
    pub contract: i64,
    pub diagnostic: i64,
    pub system: i64,
}

/// A single result from the contract ID prefix search endpoint.
#[derive(Debug, Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct ContractSearchResult {
    pub contract_id: String,
    pub event_count: i64,
    pub last_event_at: Option<DateTime<Utc>>,
}

/// Query parameters for GET /v1/contracts/search
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ContractSearchParams {
    /// Prefix to search for (minimum 4 characters).
    pub q: Option<String>,
    pub limit: Option<i64>,
}

/// Aggregate statistics for indexed events.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct EventStats {
    /// Total number of indexed events.
    pub total_events: i64,
    /// Events indexed in the last 24 hours.
    pub events_last_24h: i64,
    /// Events indexed in the last 7 days.
    pub events_last_7d: i64,
    /// Top 10 most active contracts by event count.
    pub top_contracts: Vec<ContractStatEntry>,
    /// Event count broken down by type.
    pub events_by_type: std::collections::HashMap<String, i64>,
    /// Minimum ledger sequence number in the dataset.
    pub min_ledger: Option<i64>,
    /// Maximum ledger sequence number in the dataset.
    pub max_ledger: Option<i64>,
    /// Timestamp when these statistics were computed.
    pub computed_at: DateTime<Utc>,
}

/// A single entry in the top-contracts list.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ContractStatEntry {
    pub contract_id: String,
    pub event_count: i64,
}

impl PaginationParams {
    pub const ALLOWED_FIELDS: &'static [&'static str] = &[
        "id",
        "contract_id",
        "event_type",
        "tx_hash",
        "ledger",
        "timestamp",
        "event_data",
        "event_data_normalized",
        "event_data_decoded",
        "ledger_hash",
        "in_successful_call",
        "created_at",
        "schema_version",
        "anonymized",
    ];
    pub const MAX_CONTRACT_IDS_FILTER: usize = 20;

    /// Validate a column name against the allowlist and structural constraints.
    /// Returns true if valid, false otherwise.
    pub fn validate_column_name(col: &str) -> bool {
        // Check against allowlist
        if !Self::ALLOWED_FIELDS.contains(&col) {
            return false;
        }
        // Structural check: only lowercase letters and underscores
        col.chars().all(|c| c.is_ascii_lowercase() || c == '_')
    }

    pub fn columns(&self) -> Result<Vec<&str>, (Vec<String>, Vec<&'static str>)> {
        match &self.fields {
            Some(f) if !f.trim().is_empty() => {
                let requested: Vec<&str> = f.split(',').map(|s| s.trim()).collect();
                let unknown: Vec<String> = requested
                    .iter()
                    .filter(|s| !Self::ALLOWED_FIELDS.contains(s))
                    .map(|s| s.to_string())
                    .collect();
                if !unknown.is_empty() {
                    return Err((unknown, Self::ALLOWED_FIELDS.to_vec()));
                }
                Ok(requested)
            }
            _ => Ok(Self::ALLOWED_FIELDS.to_vec()),
        }
    }
    pub fn offset(&self) -> i64 {
        let page = self.page.unwrap_or(1).max(1);
        let limit = self.limit();
        (page - 1) * limit
    }

    pub fn limit(&self) -> i64 {
        self.limit.unwrap_or(20).clamp(1, 100)
    }

    /// Validate geospatial parameters
    pub fn validate_geospatial(&self) -> Result<(), String> {
        let has_lat = self.near_lat.is_some();
        let has_lon = self.near_lon.is_some();
        let has_radius = self.radius_km.is_some();

        // All three must be provided together or none at all
        if has_lat || has_lon || has_radius {
            if !has_lat || !has_lon || !has_radius {
                return Err("near_lat, near_lon, and radius_km must all be provided together".to_string());
            }

            let lat = self.near_lat.unwrap();
            let lon = self.near_lon.unwrap();
            let radius = self.radius_km.unwrap();

            // Validate latitude range
            if lat < -90.0 || lat > 90.0 {
                return Err("near_lat must be between -90 and 90".to_string());
            }

            // Validate longitude range
            if lon < -180.0 || lon > 180.0 {
                return Err("near_lon must be between -180 and 180".to_string());
            }

            // Validate radius
            if radius <= 0.0 || radius > 20000.0 {
                return Err("radius_km must be between 0 and 20000".to_string());
            }
        }

        Ok(())
    }

    /// Validate exclusion parameters
    pub fn validate_exclusions(&self) -> Result<(), String> {
        // Cannot specify both include and exclude for the same parameter
        if self.contract_id.is_some() && self.exclude_contract_ids.is_some() {
            return Err("cannot specify both contract_id and exclude_contract_ids".to_string());
        }

        if self.contract_ids.is_some() && self.exclude_contract_ids.is_some() {
            return Err("cannot specify both contract_ids and exclude_contract_ids".to_string());
        }

        if self.event_type.is_some() && self.exclude_event_types.is_some() {
            return Err("cannot specify both event_type and exclude_event_types".to_string());
        }

        Ok(())
    }
}

/// Soroban RPC response types
#[derive(Debug, Deserialize)]
pub struct RpcResponse<T> {
    pub result: Option<T>,
    pub error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
pub struct RpcError {
    #[allow(dead_code)]
    pub code: i64,
    pub message: String,
}

/// Request body for POST /v1/admin/lua/preview
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct LuaPreviewRequest {
    /// The Lua script to preview. Must define a `transform_event(event)` function.
    pub script: String,
    /// IDs of events to apply the script to (max 20).
    pub event_ids: Vec<uuid::Uuid>,
}

/// One entry in the preview response — original event alongside the transformed result.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct LuaPreviewItem {
    pub event_id: uuid::Uuid,
    pub original: serde_json::Value,
    /// `null` when the script returned `nil` (event would be skipped).
    pub transformed: Option<serde_json::Value>,
    /// Non-null when the script raised an error for this event.
    pub error: Option<String>,
}

/// Response body for POST /v1/admin/lua/preview
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct LuaPreviewResponse {
    pub results: Vec<LuaPreviewItem>,
}

#[derive(Debug, Deserialize)]
pub struct LatestLedgerResult {
    pub sequence: u64,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetEventsResult {
    pub events: Vec<SorobanEvent>,
    #[serde(rename = "latestLedger")]
    pub latest_ledger: u64,
    #[serde(rename = "cursor")]
    pub rpc_cursor: Option<String>,
    /// Soroban protocol version returned by the RPC (used as schema_version).
    #[serde(rename = "latestLedgerCloseTime", default)]
    pub protocol_version: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SorobanEvent {
    #[serde(rename = "contractId")]
    pub contract_id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(rename = "txHash")]
    pub tx_hash: String,
    pub ledger: u64,
    #[serde(rename = "ledgerClosedAt")]
    pub ledger_closed_at: String,
    #[serde(rename = "ledgerHash", default)]
    pub ledger_hash: Option<String>,
    #[serde(rename = "inSuccessfulContractCall", default = "default_true")]
    pub in_successful_call: bool,
    pub value: Value,
    pub topic: Option<Vec<Value>>,
    /// Set by the indexer in multi-tenant mode; never serialized to JSON output.
    #[serde(skip_serializing, default)]
    pub tenant_id: Option<String>,
}

fn default_true() -> bool {
    true
}

/// A notification channel stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct NotificationChannel {
    pub id: Uuid,
    pub name: String,
    pub channel_type: String,
    pub config: Value,
    pub retry_policy: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request body for creating a notification channel.
#[derive(Debug, Deserialize)]
pub struct CreateChannelRequest {
    pub name: String,
    pub channel_type: String,
    pub config: Value,
    pub retry_policy: Option<Value>,
}

/// Request body for updating a notification channel (all fields optional.
#[derive(Debug, Deserialize, Default)]
pub struct UpdateChannelRequest {
    pub name: Option<String>,
    pub config: Option<Value>,
    pub retry_policy: Option<Value>,
}

/// Request body for cloning a channel (optional overrides applied to the copy).
#[derive(Debug, Deserialize, Default)]
pub struct CloneChannelRequest {
    pub name: Option<String>,
    pub config: Option<Value>,
    pub retry_policy: Option<Value>,
}

/// Single channel entry inside an import payload.
#[derive(Debug, Deserialize)]
pub struct ImportChannelEntry {
    pub name: String,
    pub channel_type: String,
    pub config: Value,
    pub retry_policy: Option<Value>,
}

/// Request body for POST /v1/admin/notifications/channels/import.
#[derive(Debug, Deserialize)]
pub struct ImportChannelsRequest {
    pub channels: Vec<ImportChannelEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(page: Option<i64>, limit: Option<i64>) -> PaginationParams {
        PaginationParams {
            page,
            limit,
            exact_count: None,
            fields: None,
            contract_id: None,
            contract_ids: None,
            event_type: None,
            from_ledger: None,
            to_ledger: None,
            ledger_hash: None,
            cursor: None,
            sort: None,
            sort_by: None,
            in_successful_call: None,
            schema_version: None,
            anonymized: None,
            topic_sym: None,
            topic: None,
            search: None,
            from_timestamp: None,
            to_timestamp: None,
            compact: None,
        }
    }

    #[test]
    fn page_zero_offset_is_zero() {
        assert_eq!(params(Some(0), None).offset(), 0);
    }

    #[test]
    fn page_none_offset_is_zero() {
        assert_eq!(params(None, None).offset(), 0);
    }

    #[test]
    fn limit_zero_clamps_to_one() {
        assert_eq!(params(None, Some(0)).limit(), 1);
    }

    #[test]
    fn limit_over_max_clamps_to_hundred() {
        assert_eq!(params(None, Some(200)).limit(), 100);
    }

    #[test]
    fn limit_none_defaults_to_twenty() {
        assert_eq!(params(None, None).limit(), 20);
    }

    #[test]
    fn page_3_limit_10_offset_is_20() {
        assert_eq!(params(Some(3), Some(10)).offset(), 20);
    }

    // --- RPC deserialization fixture tests ---

    #[test]
    fn deserialize_get_events_success() {
        let raw = include_str!("../tests/fixtures/get_events_response.json");
        let resp: RpcResponse<GetEventsResult> = serde_json::from_str(raw).unwrap();
        assert!(resp.error.is_none());
        let result = resp.result.unwrap();
        assert_eq!(result.latest_ledger, 1234600);
        assert_eq!(result.rpc_cursor.as_deref(), Some("1234567-0"));
        assert_eq!(result.events.len(), 1);
        let ev = &result.events[0];
        assert_eq!(
            ev.contract_id,
            "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM"
        );
        assert_eq!(ev.event_type, "contract");
        assert_eq!(
            ev.tx_hash,
            "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
        );
        assert_eq!(ev.ledger, 1234567);
        assert_eq!(ev.ledger_closed_at, "2026-03-14T00:00:00Z");
        assert!(ev.topic.is_some());
    }

    #[test]
    fn deserialize_get_events_error() {
        let raw = include_str!("../tests/fixtures/get_events_error.json");
        let resp: RpcResponse<GetEventsResult> = serde_json::from_str(raw).unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32600);
        assert_eq!(
            err.message,
            "startLedger must be within the ledger retention window"
        );
    }

    #[test]
    fn deserialize_get_events_empty() {
        let raw = include_str!("../tests/fixtures/get_events_empty.json");
        let resp: RpcResponse<GetEventsResult> = serde_json::from_str(raw).unwrap();
        assert!(resp.error.is_none());
        let result = resp.result.unwrap();
        assert!(result.events.is_empty());
        assert_eq!(result.latest_ledger, 1234600);
        assert!(result.rpc_cursor.is_none());
    }
}
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub page: i64,
    pub limit: i64,
    pub total: i64,
    pub has_more: bool,
}

// ── #511: Notification channel bulk operations ────────────────────────────────

/// A managed notification channel (in-memory store).
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct NotificationChannel {
    pub id: Uuid,
    pub name: String,
    pub channel_type: String,
    pub active: bool,
    pub tags: Vec<String>,
    /// #513: optional delivery SLA in seconds
    pub delivery_sla_seconds: Option<u64>,
    pub config: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct BulkChannelRequest {
    pub channel_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct BulkTagRequest {
    pub channel_ids: Vec<Uuid>,
    pub tags: Vec<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BulkChannelResult {
    pub id: Uuid,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BulkOperationResponse {
    pub succeeded: i64,
    pub failed: i64,
    pub results: Vec<BulkChannelResult>,
}

// ── #512: Notification system lifecycle webhooks ──────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SystemWebhookConfig {
    pub id: Uuid,
    pub url: String,
    pub secret: Option<String>,
    /// Lifecycle event types this webhook subscribes to.
    /// Supported: channel_created, channel_deleted, channel_failed,
    ///            delivery_failed, queue_backed_up, or "*" for all.
    pub events: Vec<String>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct CreateSystemWebhookRequest {
    pub url: String,
    pub secret: Option<String>,
    pub events: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LifecycleEvent {
    pub event_type: String,
    pub channel_id: Option<Uuid>,
    pub channel_name: Option<String>,
    pub message: String,
    pub occurred_at: DateTime<Utc>,
    pub metadata: Value,
}

// ── #513: Notification delivery SLA monitoring ────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct NotificationDelivery {
    pub id: Uuid,
    pub channel_id: Uuid,
    pub channel_name: String,
    pub event_indexed_at: DateTime<Utc>,
    pub delivered_at: DateTime<Utc>,
    pub latency_seconds: f64,
    pub sla_breached: bool,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct RecordDeliveryRequest {
    pub channel_id: Uuid,
    pub channel_name: String,
    pub event_indexed_at: DateTime<Utc>,
    pub delivered_at: DateTime<Utc>,
}

// ── #514: Notification capacity planning ─────────────────────────────────────

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct NotificationCapacityResponse {
    pub current_rate_per_minute: f64,
    pub projected_rate_per_minute: f64,
    pub growth_trend_percent: f64,
    pub channels: Vec<ChannelCapacityInfo>,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ChannelCapacityInfo {
    pub channel_id: Uuid,
    pub channel_name: String,
    pub current_rate_per_minute: f64,
    pub estimated_time_to_limit_minutes: Option<f64>,
    pub recommendations: Vec<String>,
}

impl<T> PaginatedResponse<T> {
    pub fn new(data: Vec<T>, page: i64, limit: i64, total: i64) -> Self {
        let has_more = (page * limit) < total;
        Self {
            data,
            page,
            limit,
            total,
            has_more,
        }
    }
}

// ── Notification Channel models (#507 #508 #509 #510) ───────────────────────

/// A managed notification channel (webhook, email, or SMS).
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct NotificationChannel {
    pub id: Uuid,
    pub name: String,
    pub channel_type: String,
    pub config: Value,
    pub retry_policy: Value,
    #[sqlx(default)]
    pub description: Option<String>,
    #[sqlx(default)]
    pub tags: Vec<String>,
    /// SHA-256 hex of the creator's API key (#508).
    #[sqlx(default)]
    pub owner: Option<String>,
    #[sqlx(default)]
    pub status: String,
    #[sqlx(default)]
    pub contract_filter: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateNotificationChannelRequest {
    pub name: String,
    pub channel_type: String,
    pub config: Value,
    #[serde(default)]
    pub retry_policy: Option<Value>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub contract_filter: Vec<String>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct UpdateNotificationChannelRequest {
    pub name: Option<String>,
    pub config: Option<Value>,
    pub retry_policy: Option<Value>,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
    pub status: Option<String>,
    pub contract_filter: Option<Vec<String>>,
}

/// Query parameters for listing/searching notification channels (#509 #510).
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct NotificationChannelSearchParams {
    /// Full-text search on name and description.
    pub q: Option<String>,
    pub channel_type: Option<String>,
    /// Filter by contract_id present in channel's contract_filter list.
    pub contract_id: Option<String>,
    pub status: Option<String>,
    /// Filter by tag (#509).
    pub tag: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

impl NotificationChannelSearchParams {
    pub fn effective_page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }
    pub fn effective_limit(&self) -> i64 {
        self.page_size.unwrap_or(20).clamp(1, 100)
    }
    pub fn offset(&self) -> i64 {
        (self.effective_page() - 1) * self.effective_limit()
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct AddTagRequest {
    pub tag: String,
}

// ── Channel Group models (#507) ──────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct NotificationChannelGroup {
    pub id: Uuid,
    pub name: String,
    #[sqlx(default)]
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateChannelGroupRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Channel IDs to include in this group.
    #[serde(default)]
    pub channel_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ChannelGroupResponse {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub channel_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}


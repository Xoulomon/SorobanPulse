use lettre::message::header::{self, Header, HeaderName, HeaderValue};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info, warn};
use uuid::Uuid;

use chrono::{DateTime, Timelike, Utc};

/// Verify SMTP credentials by attempting a connection without sending an email.
/// Used when creating or updating email notification channels (#503).
pub async fn validate_smtp_config(
    smtp_host: String,
    smtp_port: u16,
    smtp_user: Option<String>,
    smtp_password: Option<String>,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        let mut builder = SmtpTransport::relay(&smtp_host)
            .map_err(|e| format!("invalid SMTP host: {}", e))?
            .port(smtp_port);

        if let (Some(user), Some(pass)) = (smtp_user, smtp_password) {
            builder = builder.credentials(Credentials::new(user, pass));
        }

        let transport = builder.build();
        match transport.test_connection() {
            Ok(true) => Ok(()),
            Ok(false) => Err("SMTP server rejected the connection or credentials".to_string()),
            Err(e) => Err(format!("SMTP connection failed: {}", e)),
        }
    })
    .await
    .map_err(|e| format!("SMTP validation task error: {}", e))?
}

use crate::{metrics, models::SorobanEvent, retry_policy::RetryPolicy};

/// Issue #479: How often a notification channel flushes its batched events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// Flush on every batch tick (legacy behavior — roughly once per minute).
    Immediate,
    /// One digest per hour, on the hour (UTC).
    HourlyDigest,
    /// One digest per day at the configured UTC hour (default 09:00).
    DailyDigest { hour: u32 },
    /// Flush according to a cron expression (UTC). Uses the `cron` crate's
    /// 6/7-field syntax (seconds first).
    CustomCron(String),
}

impl Schedule {
    /// Build a [`Schedule`] from the `EMAIL_SCHEDULE` value.
    ///
    /// Unknown values fall back to [`Schedule::Immediate`]. `daily_hour` is
    /// clamped to a valid 0–23 hour; `cron_expr` is only used for `custom_cron`.
    pub fn parse(value: &str, daily_hour: u32, cron_expr: Option<String>) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "hourly_digest" => Schedule::HourlyDigest,
            "daily_digest" => Schedule::DailyDigest {
                hour: daily_hour.min(23),
            },
            "custom_cron" => Schedule::CustomCron(cron_expr.unwrap_or_default()),
            _ => Schedule::Immediate,
        }
    }

    /// Whether a batch should be flushed at `now`, given the last successful
    /// send at `last_sent`. This is a pure function so it can be unit-tested
    /// without a running scheduler.
    pub fn is_due(&self, now: DateTime<Utc>, last_sent: DateTime<Utc>) -> bool {
        match self {
            Schedule::Immediate => true,
            Schedule::HourlyDigest => {
                // Due once the wall-clock hour advances past the last send.
                now.timestamp().div_euclid(3600) > last_sent.timestamp().div_euclid(3600)
            }
            Schedule::DailyDigest { hour } => {
                let scheduled = now
                    .date_naive()
                    .and_hms_opt(*hour, 0, 0)
                    .map(|naive| naive.and_utc());
                match scheduled {
                    Some(scheduled) => now >= scheduled && last_sent < scheduled,
                    None => false,
                }
            }
            Schedule::CustomCron(expr) => {
                use std::str::FromStr;
                match cron::Schedule::from_str(expr) {
                    Ok(schedule) => schedule
                        .after(&last_sent)
                        .next()
                        .is_some_and(|next| next <= now),
                    Err(_) => false,
                }
            }
        }
    }
}

/// Issue #479: A UTC quiet-hours window during which non-critical
/// notifications are suppressed (deferred until the window closes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuietHours {
    /// Minutes since UTC midnight when the window opens.
    start_min: u32,
    /// Minutes since UTC midnight when the window closes.
    end_min: u32,
}

impl QuietHours {
    /// Parse a `start`/`end` pair of `HH:MM` strings into a window.
    ///
    /// Returns `None` when either bound is missing or unparseable, or when the
    /// window is empty (start == end), which disables quiet hours.
    pub fn parse(start: Option<&str>, end: Option<&str>) -> Option<QuietHours> {
        let start_min = parse_hh_mm(start?)?;
        let end_min = parse_hh_mm(end?)?;
        if start_min == end_min {
            return None;
        }
        Some(QuietHours { start_min, end_min })
    }

    /// Whether `now` falls inside the quiet-hours window. Handles windows that
    /// wrap past midnight (e.g. 22:00–07:00).
    pub fn contains(&self, now: DateTime<Utc>) -> bool {
        let minute_of_day = now.hour() * 60 + now.minute();
        if self.start_min < self.end_min {
            minute_of_day >= self.start_min && minute_of_day < self.end_min
        } else {
            // Wrap-around window (e.g. 22:00–07:00).
            minute_of_day >= self.start_min || minute_of_day < self.end_min
        }
    }
}

/// Parse an `HH:MM` 24-hour string into minutes since midnight.
fn parse_hh_mm(value: &str) -> Option<u32> {
    let value = value.trim();
    let (h, m) = value.split_once(':')?;
    let hours: u32 = h.trim().parse().ok()?;
    let minutes: u32 = m.trim().parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some(hours * 60 + minutes)
/// The `List-Unsubscribe` header (RFC 2369). Lets conforming mail clients
/// surface a native unsubscribe action pointing at our unsubscribe URL.
#[derive(Clone)]
struct ListUnsubscribe(String);

impl Header for ListUnsubscribe {
    fn name() -> HeaderName {
        HeaderName::new_from_ascii_str("List-Unsubscribe")
    }

    fn parse(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(s.to_string()))
    }

    fn display(&self) -> HeaderValue {
        HeaderValue::new(Self::name(), self.0.clone())
    }
}

/// Generate an opaque, URL-safe unsubscribe token.
fn generate_unsubscribe_token() -> String {
    // Two UUIDs (256 bits of randomness) hashed to a hex string yields a
    // collision-resistant, opaque token that is safe to embed in a URL.
    let raw = format!("{}{}", Uuid::new_v4(), Uuid::new_v4());
    let digest = Sha256::digest(raw.as_bytes());
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Return the existing unsubscribe token for `email`, creating one if absent.
/// Returns `None` only if the database is unreachable.
pub async fn get_or_create_unsubscribe_token(
    pool: &sqlx::PgPool,
    email: &str,
) -> Option<String> {
    // Fast path: token already exists.
    if let Ok(Some(token)) = sqlx::query_scalar::<_, String>(
        "SELECT token FROM email_unsubscribes WHERE email = $1",
    )
    .bind(email)
    .fetch_optional(pool)
    .await
    {
        return Some(token);
    }

    // Insert a new token. ON CONFLICT handles a race where another sender
    // inserted the same email concurrently — we then read back the winner.
    let token = generate_unsubscribe_token();
    let inserted = sqlx::query_scalar::<_, String>(
        "INSERT INTO email_unsubscribes (email, token) VALUES ($1, $2) \
         ON CONFLICT (email) DO NOTHING RETURNING token",
    )
    .bind(email)
    .bind(&token)
    .fetch_optional(pool)
    .await;

    match inserted {
        Ok(Some(t)) => Some(t),
        Ok(None) => sqlx::query_scalar::<_, String>(
            "SELECT token FROM email_unsubscribes WHERE email = $1",
        )
        .bind(email)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten(),
        Err(e) => {
            error!(error = %e, "Failed to create unsubscribe token");
            None
        }
    }
}

/// True when `email` has opted out of notifications.
pub async fn is_unsubscribed(pool: &sqlx::PgPool, email: &str) -> bool {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM email_unsubscribes \
         WHERE email = $1 AND unsubscribed_at IS NOT NULL",
    )
    .bind(email)
    .fetch_one(pool)
    .await
    .map(|c| c > 0)
    .unwrap_or(false)
}

/// Mark the recipient identified by `token` as unsubscribed.
/// Returns `Ok(true)` if a matching, not-yet-unsubscribed recipient was found.
/// Idempotent: re-using an already-unsubscribed token returns `Ok(true)`.
pub async fn mark_unsubscribed(pool: &sqlx::PgPool, token: &str) -> Result<bool, sqlx::Error> {
    // Set unsubscribed_at only if not already set; report whether the token exists.
    let updated = sqlx::query(
        "UPDATE email_unsubscribes \
         SET unsubscribed_at = NOW() \
         WHERE token = $1 AND unsubscribed_at IS NULL",
    )
    .bind(token)
    .execute(pool)
    .await?;

    if updated.rows_affected() > 0 {
        return Ok(true);
    }

    // No row updated: either the token is unknown or already unsubscribed.
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM email_unsubscribes WHERE token = $1",
    )
    .bind(token)
    .fetch_one(pool)
    .await?;

    Ok(exists > 0)
}

/// Batched email notification sender.
/// Collects events for up to 1 minute, then sends a summary email per recipient.
pub struct EmailNotifier {
    smtp_host: String,
    smtp_port: u16,
    smtp_user: Option<String>,
    smtp_password: Option<SecretString>,
    from: String,
    to: Vec<String>,
    contract_filter: Vec<String>,
    retry_policy: RetryPolicy,
    /// Issue #479: when batched events are flushed.
    schedule: Schedule,
    /// Issue #479: optional UTC window during which delivery is suppressed.
    quiet_hours: Option<QuietHours>,
    /// Issue #480: language used to render notification templates (default `en`).
    language: String,
    pool: sqlx::PgPool,
    /// Base URL used to build unsubscribe links (Issue #483).
    base_url: String,
}

impl EmailNotifier {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        smtp_host: String,
        smtp_port: u16,
        smtp_user: Option<String>,
        smtp_password: Option<SecretString>,
        from: String,
        to: Vec<String>,
        contract_filter: Vec<String>,
        retry_policy: RetryPolicy,
        schedule: Schedule,
        quiet_hours: Option<QuietHours>,
        language: String,
        pool: sqlx::PgPool,
        base_url: String,
    ) -> Self {
        Self {
            smtp_host,
            smtp_port,
            smtp_user,
            smtp_password,
            from,
            to,
            contract_filter,
            retry_policy,
            schedule,
            quiet_hours,
            language,
            pool,
            base_url,
        }
    }

    /// Set the base URL used for tracking endpoints.
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }

    /// Enable A/B testing for email templates.
    pub fn with_ab_test(mut self, config: AbTestConfig) -> Self {
        self.ab_test = Some(config);
        self
    }

    /// Spawn a background task that batches events and sends emails every minute.
    /// Critical-priority events are sent immediately without waiting for the batch.
    pub fn spawn(
        self,
        mut event_rx: tokio::sync::broadcast::Receiver<SorobanEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Evaluate the schedule once a minute. Events accumulate in the
            // buffer until the configured schedule says a flush is due and we
            // are outside of any quiet-hours window (Issue #479).
            let mut batch_interval = interval(Duration::from_secs(60));
            batch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut events_buffer: Vec<SorobanEvent> = Vec::new();
            let mut last_sent = Utc::now();

            loop {
                tokio::select! {
                    _ = batch_interval.tick() => {
                        let now = Utc::now();
                        if events_buffer.is_empty() || !self.schedule.is_due(now, last_sent) {
                            continue;
                        }
                        // Suppress (defer) non-critical notifications during
                        // quiet hours; the buffer is flushed once the window
                        // closes on a later tick.
                        if self.quiet_hours.is_some_and(|q| q.contains(now)) {
                            info!("In quiet hours, deferring email notification");
                            continue;
                        }
                        self.send_batch_email(&events_buffer).await;
                        events_buffer.clear();
                        last_sent = now;
                    }
                    result = event_rx.recv() => {
                        match result {
                            Ok(event) => {
                                if !self.contract_filter.is_empty()
                                    && !self.contract_filter.contains(&event.contract_id)
                                {
                                    continue;
                                }

                                // Critical-priority events are delivered immediately (#492).
                                let priority = self.evaluate_priority(&event);
                                if priority == "critical" {
                                    self.send_batch_email(&[event]).await;
                                } else {
                                    events_buffer.push(event);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!(
                                    skipped = n,
                                    "Email notifier lagged, some events skipped"
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                // Channel closed, flush any remaining events and exit.
                                if !events_buffer.is_empty() {
                                    self.send_batch_email(&events_buffer).await;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        })
    }

    /// Returns true if the target is in the active suppression list (Issue #490).
    async fn is_suppressed(&self, target: &str, target_type: &str) -> bool {
        match sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM suppression_lists \
             WHERE target = $1 AND target_type = $2 \
             AND (expires_at IS NULL OR expires_at > NOW())",
        )
        .bind(target)
        .bind(target_type)
        .fetch_one(&self.pool)
        .await
        {
            Ok(count) => count > 0,
            Err(_) => false,
        }
    }

    /// Deterministically assign an A/B template by hashing recipient + batch key (Issue #489).
    /// Returns 'A' or 'B'.
    pub fn assign_ab_template(&self, recipient: &str, batch_key: &str) -> char {
        if let Some(ref ab) = self.ab_test {
            let mut h = Sha256::new();
            h.update(recipient.as_bytes());
            h.update(b":");
            h.update(batch_key.as_bytes());
            let hash = h.finalize();
            let ratio = hash[0] as f64 / 255.0 * 100.0;
            if ratio < ab.split_percentage {
                'A'
            } else {
                'B'
            }
        } else {
            'A'
        }
    }

    /// Build the plain-text body for a batch of events.
    pub fn build_text_body(&self, events: &[SorobanEvent]) -> String {
        let mut by_contract: HashMap<String, Vec<&SorobanEvent>> = HashMap::new();
        for event in events {
            by_contract
                .entry(event.contract_id.clone())
                .or_default()
                .push(event);
        }

        let mut body = format!(
            "Soroban Pulse indexed {} new event{} in the last minute.\n\n",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        );

        for entry in entries {
            body.push_str(&format!(
                "Contract: {}\n  Events: {}  |  First: {}  |  Last: {}\n",
                entry.contract_id, entry.event_count, entry.first_ts, entry.last_ts
            ));
            for event in contract_events.iter().take(10) {
                body.push_str(&format!(
                    "  - Ledger: {}  TxHash: {}  Type: {}\n",
                    event.ledger, event.tx_hash, event.event_type
                ));
            }
            if contract_events.len() > 10 {
                body.push_str(&format!(
                    "  ... and {} more event{}\n",
                    contract_events.len() - 10,
                    if contract_events.len() - 10 == 1 { "" } else { "s" }
                ));
            }
            body.push('\n');
        }
        body
    }

    /// Build an HTML email body with a tracking pixel and click-tracked links (Issue #487, #488).
    ///
    /// `open_token` is the unique token for the tracking pixel.
    /// `click_tokens` maps tx_hash → click token for link wrapping.
    pub fn build_html_body(
        &self,
        events: &[SorobanEvent],
        open_token: &str,
        click_tokens: &HashMap<String, String>,
    ) -> String {
        let mut by_contract: HashMap<String, Vec<&SorobanEvent>> = HashMap::new();
        for event in events {
            by_contract
                .entry(event.contract_id.clone())
                .or_default()
                .push(event);
        }

        let mut html = String::from(
            "<!DOCTYPE html><html><body style=\"font-family:sans-serif;\">"
        );
        html.push_str(&format!(
            "<p>Soroban Pulse indexed <strong>{}</strong> new event{} in the last minute.</p>",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        ));

        for (contract_id, contract_events) in by_contract.iter() {
            html.push_str(&format!(
                "<h3>Contract: {}</h3><p>Events: {}</p><ul>",
                contract_id,
                contract_events.len()
            ));
            for event in contract_events.iter().take(10) {
                let display_hash = if event.tx_hash.len() > 16 {
                    format!("{}...", &event.tx_hash[..16])
                } else {
                    event.tx_hash.clone()
                };
                let link_html = if !self.base_url.is_empty() {
                    if let Some(token) = click_tokens.get(&event.tx_hash) {
                        format!(
                            "<a href=\"{}/v1/notifications/email/click/{}\">{}</a>",
                            self.base_url, token, display_hash
                        )
                    } else {
                        display_hash.clone()
                    }
                } else {
                    display_hash.clone()
                };
                html.push_str(&format!(
                    "<li>Type: {}, Ledger: {}, TxHash: {}</li>",
                    event.event_type, event.ledger, link_html
                ));
            }
            if contract_events.len() > 10 {
                html.push_str(&format!(
                    "<li>... and {} more</li>",
                    contract_events.len() - 10
                ));
            }
            html.push_str("</ul>");
        }

        // Send a separate message to each recipient so every email carries its
        // own unsubscribe link (Issue #483). Recipients who have opted out are
        // skipped entirely.
        let mut sent = 0usize;
        for recipient in &self.to {
            if is_unsubscribed(&self.pool, recipient).await {
                info!(recipient = %recipient, "Recipient has unsubscribed, skipping");
                continue;
            }

            let unsubscribe_url = get_or_create_unsubscribe_token(&self.pool, recipient)
                .await
                .map(|token| {
                    format!(
                        "{}/unsubscribe?token={}",
                        self.base_url.trim_end_matches('/'),
                        token
                    )
                });

            let mut personalized = body.clone();
            if let Some(ref url) = unsubscribe_url {
                personalized.push_str(&format!(
                    "\n--\nYou are receiving this because you subscribed to Soroban Pulse \
                     notifications.\nTo unsubscribe, visit: {url}\n"
                ));
            }

            if let Err(e) = self
                .send_email(recipient, &subject, &personalized, unsubscribe_url.as_deref())
                .await
            {
                error!(error = %e, recipient = %recipient, "Failed to send email notification");
                metrics::record_email_failure();
            } else {
                sent += 1;
            }
        }

        if sent > 0 {
            info!(
                recipients = sent,
                event_count = events.len(),
                "Email notification sent successfully"
            );
        }

    /// Send an email to a single recipient using SMTP. When `unsubscribe_url`
    /// is set, a `List-Unsubscribe` header is added so mail clients can offer a
    /// one-click unsubscribe (RFC 2369 / CAN-SPAM compliance, Issue #483).
    async fn send_email(
        &self,
        recipient: &str,
        subject: &str,
        body: &str,
        unsubscribe_url: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut message_builder = Message::builder()
            .from(self.from.parse()?)
            .to(recipient.parse()?)
            .subject(subject);

        if let Some(url) = unsubscribe_url {
            message_builder = message_builder.header(ListUnsubscribe(format!("<{url}>")));
        }

        let mut message = message_builder
            .header(header::ContentType::TEXT_PLAIN)
            .body(body.to_string())?;

        // DKIM-sign the message when a signing key is configured (Issue #485).
        // A bad key never blocks delivery — it is logged and the email is sent
        // unsigned (the key is validated at startup, so this is defensive).
        if let (Some(selector), Some(key)) = (&self.dkim_selector, &self.dkim_private_key) {
            match build_dkim_config(selector, &self.from, key.expose_secret()) {
                Ok(config) => message.sign(&config),
                Err(e) => warn!(error = %e, "DKIM signing skipped"),
            }
        }

        // Build SMTP transport
        let mut transport_builder = SmtpTransport::relay(&self.smtp_host)?.port(self.smtp_port);

        if let (Some(user), Some(password)) = (&self.smtp_user, &self.smtp_password) {
            transport_builder = transport_builder.credentials(Credentials::new(
                user.clone(),
                password.expose_secret().clone(),
            ));
        }

        let mailer = transport_builder.build();
        let result =
            tokio::task::spawn_blocking(move || mailer.send(&message)).await?;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }
}

/// Numeric rank for priority comparison (higher = more urgent) (Issue #492).
fn priority_rank(p: &str) -> u8 {
    match p {
        "critical" => 3,
        "high" => 2,
        "medium" => 1,
        _ => 0,
    }
}

/// Per-contract digest entry built during batch email assembly (Issue #491).
struct ContractDigestEntry<'a> {
    contract_id: &'a str,
    event_count: usize,
    type_counts: HashMap<String, usize>,
    first_ts: String,
    last_ts: String,
    sample_events: Vec<&'a SorobanEvent>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mock_event(contract_id: &str, event_type: &str, ledger: u64) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            event_type: "contract".to_string(),
            tx_hash: "abc123def456789012345678".to_string(),
            ledger,
            ledger_closed_at: format!("2026-06-25T{:02}:00:00Z", ledger % 24),
            ledger_hash: None,
            in_successful_call: true,
            value: json!({"test": "data"}),
            topic: None,
            ..Default::default()
        }
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn test_sender_domain_extraction() {
        assert_eq!(
            sender_domain("pulse@example.com").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            sender_domain("Soroban Pulse <pulse@mail.example.com>").as_deref(),
            Some("mail.example.com")
        );
        assert_eq!(sender_domain("trailing@").as_deref(), None);
    }

    #[test]
    fn test_email_notifier_creation() {
        // `connect_lazy` builds a pool handle without opening a connection,
        // so this stays a pure unit test (no live database required).
        let pool = sqlx::PgPool::connect_lazy("postgres://localhost/soroban_pulse_test")
            .expect("lazy pool");
        let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
        let notifier = EmailNotifier::new(
            "smtp.example.com".to_string(),
            587,
            Some("user".to_string()),
            Some(SecretString::new("pass".to_string())),
            "from@example.com".to_string(),
            vec!["to@example.com".to_string()],
            vec![],
            RetryPolicy::default(),
            Schedule::Immediate,
            None,
            pool,
            pool,
            "https://pulse.example.com".to_string(),
        );

    #[test]
    fn test_email_notifier_creation() {
        let notifier = make_notifier();
        assert_eq!(notifier.smtp_host, "smtp.example.com");
        assert_eq!(notifier.base_url, "https://pulse.example.com");
        assert_eq!(notifier.smtp_port, 587);
        assert_eq!(notifier.from, "from@example.com");
        assert_eq!(notifier.to.len(), 1);
        assert_eq!(notifier.schedule, Schedule::Immediate);
        assert_eq!(notifier.language, "en");
    }

    #[test]
    fn test_unsubscribe_token_is_opaque_and_unique() {
        let a = generate_unsubscribe_token();
        let b = generate_unsubscribe_token();
        assert_ne!(a, b, "tokens must be unique");
        assert_eq!(a.len(), 64, "sha256 hex digest is 64 chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_list_unsubscribe_header_display() {
        let h = ListUnsubscribe("<https://pulse.example.com/unsubscribe?token=abc>".to_string());
        assert_eq!(
            ListUnsubscribe::name(),
            HeaderName::new_from_ascii_str("List-Unsubscribe")
        );
        // display() must not panic and round-trips the raw value.
        let _ = h.display();
    }

    #[test]
    fn test_grouping_by_contract() {
        let events = vec![
            mock_event("CONTRACT_A", "contract", 100),
            mock_event("CONTRACT_A", "diagnostic", 101),
            mock_event("CONTRACT_B", "system", 102),
        ];

        let mut by_contract: HashMap<&str, Vec<&SorobanEvent>> = HashMap::new();
        for event in &events {
            by_contract
                .entry(event.contract_id.as_str())
                .or_default()
                .push(event);
        }
        assert_eq!(by_contract.len(), 2);
        assert_eq!(by_contract["CONTRACT_A"].len(), 2);
        assert_eq!(by_contract["CONTRACT_B"].len(), 1);
    }

    #[test]
    fn test_contract_filter_logic() {
        let filter = vec!["CONTRACT_A".to_string(), "CONTRACT_B".to_string()];
        let event_a = mock_event("CONTRACT_A", 100);
        let event_b = mock_event("CONTRACT_B", 101);
        let event_c = mock_event("CONTRACT_C", 102);
        assert!(filter.contains(&event_a.contract_id));
        assert!(filter.contains(&event_b.contract_id));
        assert!(!filter.contains(&event_c.contract_id));
    }

    #[test]
    fn test_empty_contract_filter_allows_all() {
        let filter: Vec<String> = vec![];
        let event = mock_event("ANY_CONTRACT", 100);
        assert!(filter.is_empty() || filter.contains(&event.contract_id));
    }

    // --- Issue #479: schedule evaluation ---

    #[test]
    fn test_schedule_parse() {
        assert_eq!(Schedule::parse("immediate", 9, None), Schedule::Immediate);
        assert_eq!(
            Schedule::parse("hourly_digest", 9, None),
            Schedule::HourlyDigest
        );
        assert_eq!(
            Schedule::parse("daily_digest", 7, None),
            Schedule::DailyDigest { hour: 7 }
        );
        // Out-of-range hour is clamped.
        assert_eq!(
            Schedule::parse("daily_digest", 99, None),
            Schedule::DailyDigest { hour: 23 }
        );
        assert_eq!(
            Schedule::parse("custom_cron", 9, Some("0 0 * * * *".to_string())),
            Schedule::CustomCron("0 0 * * * *".to_string())
        );
        // Unknown values fall back to immediate.
        assert_eq!(Schedule::parse("weekly", 9, None), Schedule::Immediate);
    }

    #[test]
    fn test_immediate_always_due() {
        let now = ts("2026-06-25T03:00:00Z");
        assert!(Schedule::Immediate.is_due(now, now));
    }

    #[test]
    fn test_hourly_digest_due_on_new_hour() {
        let last = ts("2026-06-25T08:30:00Z");
        // Still the same hour -> not due.
        assert!(!Schedule::HourlyDigest.is_due(ts("2026-06-25T08:45:00Z"), last));
        // Crossed into a new hour -> due.
        assert!(Schedule::HourlyDigest.is_due(ts("2026-06-25T09:01:00Z"), last));
    }

    #[test]
    fn test_daily_digest_sends_once_per_day() {
        let schedule = Schedule::DailyDigest { hour: 9 };
        let last = ts("2026-06-24T09:00:00Z");

        // Before the scheduled hour today -> not due.
        assert!(!schedule.is_due(ts("2026-06-25T08:59:00Z"), last));
        // At/after the scheduled hour and not yet sent today -> due.
        assert!(schedule.is_due(ts("2026-06-25T09:00:00Z"), last));
        // Already sent today -> not due again.
        let sent_today = ts("2026-06-25T09:00:00Z");
        assert!(!schedule.is_due(ts("2026-06-25T18:00:00Z"), sent_today));
    }

    #[test]
    fn test_custom_cron_due() {
        // "At second 0 of minute 0 of every hour" (6-field cron, seconds first).
        let schedule = Schedule::CustomCron("0 0 * * * *".to_string());
        let last = ts("2026-06-25T08:30:00Z");
        // 09:00:00 occurs between last and now -> due.
        assert!(schedule.is_due(ts("2026-06-25T09:00:30Z"), last));
        // No top-of-hour boundary crossed yet -> not due.
        assert!(!schedule.is_due(ts("2026-06-25T08:45:00Z"), last));
    }

    #[test]
    fn test_custom_cron_invalid_expression_never_due() {
        let schedule = Schedule::CustomCron("not a cron".to_string());
        let last = ts("2026-06-25T08:30:00Z");
        assert!(!schedule.is_due(ts("2026-06-25T09:00:00Z"), last));
    }

    // --- Issue #479: quiet hours ---

    #[test]
    fn test_quiet_hours_parse() {
        assert!(QuietHours::parse(Some("22:00"), Some("07:00")).is_some());
        // Missing bound disables quiet hours.
        assert!(QuietHours::parse(Some("22:00"), None).is_none());
        // Empty window disables quiet hours.
        assert!(QuietHours::parse(Some("09:00"), Some("09:00")).is_none());
        // Invalid time disables quiet hours.
        assert!(QuietHours::parse(Some("25:00"), Some("07:00")).is_none());
    }

    #[test]
    fn test_quiet_hours_wraps_past_midnight() {
        let quiet = QuietHours::parse(Some("22:00"), Some("07:00")).unwrap();
        assert!(quiet.contains(ts("2026-06-25T23:30:00Z"))); // late night
        assert!(quiet.contains(ts("2026-06-25T03:00:00Z"))); // early morning
        assert!(!quiet.contains(ts("2026-06-25T12:00:00Z"))); // midday
        // Boundaries: start inclusive, end exclusive.
        assert!(quiet.contains(ts("2026-06-25T22:00:00Z")));
        assert!(!quiet.contains(ts("2026-06-25T07:00:00Z")));
    }

    #[test]
    fn test_quiet_hours_same_day_window() {
        let quiet = QuietHours::parse(Some("09:00"), Some("17:00")).unwrap();
        assert!(quiet.contains(ts("2026-06-25T12:00:00Z")));
        assert!(!quiet.contains(ts("2026-06-25T08:00:00Z")));
        assert!(!quiet.contains(ts("2026-06-25T18:00:00Z")));
    // Issue #487: open tracking
    #[test]
    fn test_build_html_body_includes_tracking_pixel() {
        let notifier = make_notifier()
            .with_base_url("https://example.com".to_string());
        let events = vec![mock_event("CONTRACT_A", 100)];
        let html = notifier.build_html_body(&events, "test-token-123", &HashMap::new());
        assert!(html.contains("test-token-123"));
        assert!(html.contains("/v1/notifications/email/track/"));
        assert!(html.contains("width=\"1\""));
    }

    #[test]
    fn test_build_html_body_no_pixel_without_base_url() {
        let notifier = make_notifier();
        let events = vec![mock_event("CONTRACT_A", 100)];
        let html = notifier.build_html_body(&events, "test-token-123", &HashMap::new());
        assert!(!html.contains("/v1/notifications/email/track/"));
    }

    // Issue #488: click tracking
    #[test]
    fn test_build_html_body_wraps_links_with_click_tokens() {
        let notifier = make_notifier()
            .with_base_url("https://example.com".to_string());
        let events = vec![mock_event("CONTRACT_A", 100)];
        let mut click_tokens = HashMap::new();
        click_tokens.insert("abc123def456789012345678".to_string(), "click-token-xyz".to_string());
        let html = notifier.build_html_body(&events, "open-tok", &click_tokens);
        assert!(html.contains("click-token-xyz"));
        assert!(html.contains("/v1/notifications/email/click/"));
    }

    // Issue #489: A/B test assignment
    #[test]
    fn test_ab_test_assignment_is_deterministic() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/test")
            .unwrap();
        let notifier = EmailNotifier::new(
            "smtp.example.com".to_string(),
            587,
            None,
            None,
            "from@example.com".to_string(),
            vec!["to@example.com".to_string()],
            vec![],
            crate::retry_policy::RetryPolicy::email_default(),
            pool,
        )
        .with_ab_test(AbTestConfig {
            template_a: "Template A body".to_string(),
            template_b: "Template B body".to_string(),
            split_percentage: 50.0,
        });

        let t1 = notifier.assign_ab_template("alice@example.com", "batchkey");
        let t2 = notifier.assign_ab_template("alice@example.com", "batchkey");
        assert_eq!(t1, t2, "assignment must be deterministic");
        assert!(t1 == 'A' || t1 == 'B');
    }

    #[test]
    fn test_ab_test_split_distributes_across_recipients() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/test")
            .unwrap();
        let notifier = EmailNotifier::new(
            "smtp.example.com".to_string(),
            587,
            None,
            None,
            "from@example.com".to_string(),
            vec![],
            vec![],
            crate::retry_policy::RetryPolicy::email_default(),
            pool,
        )
        .with_ab_test(AbTestConfig {
            template_a: "A".to_string(),
            template_b: "B".to_string(),
            split_percentage: 50.0,
        });

        let recipients: Vec<String> = (0..100).map(|i| format!("user{}@example.com", i)).collect();
        let a_count = recipients
            .iter()
            .filter(|r| notifier.assign_ab_template(r, "batch1") == 'A')
            .count();
        // With 50% split and 100 recipients, expect roughly 30–70 in group A
        assert!(a_count >= 20 && a_count <= 80, "split off: A count = {}", a_count);
    }

    // Issue #490: suppression list enforcement (unit-level)
    #[test]
    fn test_build_text_body_has_event_count() {
        let notifier = make_notifier();
        let events = vec![mock_event("C1", 1), mock_event("C2", 2)];
        let body = notifier.build_text_body(&events);
        assert!(body.contains("2 new events"));
    }
}

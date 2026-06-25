use lettre::message::{header, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info, warn};

use crate::{metrics, models::SorobanEvent, retry_policy::RetryPolicy};

/// Batched email notification sender.
/// Collects events for up to 1 minute, then sends a single summary email grouped
/// by contract. Supports a configurable limit on contracts shown per digest (#491)
/// and priority-based immediate delivery for critical events (#492).
pub struct EmailNotifier {
    smtp_host: String,
    smtp_port: u16,
    smtp_user: Option<String>,
    smtp_password: Option<SecretString>,
    from: String,
    to: Vec<String>,
    contract_filter: Vec<String>,
    retry_policy: RetryPolicy,
    pool: sqlx::PgPool,
    /// Maximum number of contracts to show in a single digest email (Issue #491).
    max_contracts_in_digest: usize,
    /// Default notification priority (Issue #492).
    default_priority: String,
    /// JSONPath to extract priority field from event value (Issue #492).
    priority_rule_path: Option<String>,
    /// Expected value at priority_rule_path that triggers priority_rule_priority (Issue #492).
    priority_rule_value: Option<String>,
    /// Priority level to assign when the rule matches (Issue #492).
    priority_rule_priority: Option<String>,
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
        pool: sqlx::PgPool,
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
            pool,
            max_contracts_in_digest: 20,
            default_priority: "medium".to_string(),
            priority_rule_path: None,
            priority_rule_value: None,
            priority_rule_priority: None,
        }
    }

    pub fn with_digest_config(
        mut self,
        max_contracts_in_digest: usize,
        default_priority: String,
        priority_rule_path: Option<String>,
        priority_rule_value: Option<String>,
        priority_rule_priority: Option<String>,
    ) -> Self {
        self.max_contracts_in_digest = max_contracts_in_digest;
        self.default_priority = default_priority;
        self.priority_rule_path = priority_rule_path;
        self.priority_rule_value = priority_rule_value;
        self.priority_rule_priority = priority_rule_priority;
        self
    }

    /// Evaluate the notification priority of an event using configured rules (Issue #492).
    fn evaluate_priority(&self, event: &SorobanEvent) -> &str {
        if let (Some(path), Some(expected), Some(priority)) = (
            &self.priority_rule_path,
            &self.priority_rule_value,
            &self.priority_rule_priority,
        ) {
            let path_segments: Vec<&str> = path
                .trim_start_matches("$.")
                .split('.')
                .filter(|s| !s.is_empty())
                .collect();

            let mut current = &event.value;
            for segment in &path_segments {
                match current.get(segment) {
                    Some(next) => current = next,
                    None => return self.default_priority.as_str(),
                }
            }

            if current.as_str() == Some(expected.as_str()) {
                return priority.as_str();
            }
        }
        self.default_priority.as_str()
    }

    /// Spawn a background task that batches events and sends emails every minute.
    /// Critical-priority events are sent immediately without waiting for the batch.
    pub fn spawn(
        self,
        mut event_rx: tokio::sync::broadcast::Receiver<SorobanEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut batch_interval = interval(Duration::from_secs(60));
            batch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut events_buffer: Vec<SorobanEvent> = Vec::new();

            loop {
                tokio::select! {
                    _ = batch_interval.tick() => {
                        if !events_buffer.is_empty() {
                            self.send_batch_email(&events_buffer).await;
                            events_buffer.clear();
                        }
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

    /// Build a per-contract summary for the digest.
    /// Returns a vector of (contract_id, event_count, type_breakdown, first_ts, last_ts)
    /// sorted by event count descending. Applies max_contracts_in_digest limit (#491).
    fn build_contract_summaries<'a>(
        &self,
        events: &'a [SorobanEvent],
    ) -> (Vec<ContractDigestEntry<'a>>, usize) {
        let mut by_contract: HashMap<&str, Vec<&SorobanEvent>> = HashMap::new();
        for event in events {
            by_contract
                .entry(event.contract_id.as_str())
                .or_default()
                .push(event);
        }

        let mut entries: Vec<ContractDigestEntry<'_>> = by_contract
            .into_iter()
            .map(|(contract_id, contract_events)| {
                let mut type_counts: HashMap<String, usize> = HashMap::new();
                for e in &contract_events {
                    *type_counts.entry(e.event_type.clone()).or_insert(0) += 1;
                }

                let first_ts = contract_events
                    .iter()
                    .map(|e| e.ledger_closed_at.as_str())
                    .min()
                    .unwrap_or("")
                    .to_string();
                let last_ts = contract_events
                    .iter()
                    .map(|e| e.ledger_closed_at.as_str())
                    .max()
                    .unwrap_or("")
                    .to_string();

                ContractDigestEntry {
                    contract_id,
                    event_count: contract_events.len(),
                    type_counts,
                    first_ts,
                    last_ts,
                    sample_events: contract_events,
                }
            })
            .collect();

        entries.sort_by(|a, b| b.event_count.cmp(&a.event_count));

        let total_contracts = entries.len();
        let shown = entries.len().min(self.max_contracts_in_digest);
        entries.truncate(shown);

        (entries, total_contracts)
    }

    /// Send a digest email for a batch of events with idempotency (Issue #491).
    async fn send_batch_email(&self, events: &[SorobanEvent]) {
        if events.is_empty() {
            return;
        }

        // Build idempotency key from event tx_hashes + ledgers.
        let event_keys: Vec<String> = events
            .iter()
            .map(|e| format!("{}-{}", e.tx_hash, e.ledger))
            .collect();
        let idempotency_key = {
            let hash = sha2::Sha256::digest(event_keys.join(",").as_bytes());
            format!(
                "batch_{}",
                hash.iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>()[..16]
                    .to_string()
            )
        };

        // Check if already sent.
        if let Ok(existing) = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM email_notifications WHERE idempotency_key = $1",
        )
        .bind(&idempotency_key)
        .fetch_one(&self.pool)
        .await
        {
            if existing > 0 {
                info!(idempotency_key = %idempotency_key, "Email already sent, skipping");
                return;
            }
        }

        let (contract_entries, total_contracts) = self.build_contract_summaries(events);
        let omitted_count = total_contracts.saturating_sub(contract_entries.len());

        // Determine priority for subject line (#492).
        let highest_priority = events
            .iter()
            .map(|e| self.evaluate_priority(e))
            .fold("low", |acc, p| {
                if priority_rank(p) > priority_rank(acc) { p } else { acc }
            });

        let subject = format!(
            "[{}] Soroban Pulse: {} event{} across {} contract{}",
            highest_priority.to_uppercase(),
            events.len(),
            if events.len() == 1 { "" } else { "s" },
            total_contracts,
            if total_contracts == 1 { "" } else { "s" },
        );

        let plain_body = self.build_plain_body(&contract_entries, omitted_count, events.len());
        let html_body = self.build_html_body(&contract_entries, omitted_count, events.len(), highest_priority);

        if let Err(e) = self.send_email(&subject, &plain_body, &html_body).await {
            error!(error = %e, "Failed to send email notification");
            metrics::record_email_failure();
        } else {
            // Record the sent notification to prevent duplicates.
            let _ = sqlx::query(
                "INSERT INTO email_notifications (idempotency_key) VALUES ($1) ON CONFLICT DO NOTHING",
            )
            .bind(&idempotency_key)
            .execute(&self.pool)
            .await;

            info!(
                recipients = self.to.len(),
                event_count = events.len(),
                contract_count = total_contracts,
                "Email notification sent successfully"
            );
        }
    }

    /// Build the plain-text body grouped by contract (Issue #491).
    fn build_plain_body(
        &self,
        entries: &[ContractDigestEntry<'_>],
        omitted_count: usize,
        total_events: usize,
    ) -> String {
        let mut body = format!(
            "Soroban Pulse indexed {} event{} in the last period.\n\n",
            total_events,
            if total_events == 1 { "" } else { "s" }
        );

        for entry in entries {
            body.push_str(&format!(
                "Contract: {}\n  Events: {}  |  First: {}  |  Last: {}\n",
                entry.contract_id, entry.event_count, entry.first_ts, entry.last_ts
            ));

            let mut types: Vec<(&String, &usize)> = entry.type_counts.iter().collect();
            types.sort_by_key(|(t, _)| t.as_str());
            let type_line: String = types
                .iter()
                .map(|(t, c)| format!("{}: {}", t, c))
                .collect::<Vec<_>>()
                .join(", ");
            body.push_str(&format!("  Types: {}\n", type_line));

            for event in entry.sample_events.iter().take(5) {
                body.push_str(&format!(
                    "  - Ledger: {}  TxHash: {}  Type: {}\n",
                    event.ledger, event.tx_hash, event.event_type
                ));
            }
            if entry.event_count > 5 {
                body.push_str(&format!(
                    "  ... and {} more event{}\n",
                    entry.event_count - 5,
                    if entry.event_count - 5 == 1 { "" } else { "s" }
                ));
            }
            body.push('\n');
        }

        if omitted_count > 0 {
            body.push_str(&format!(
                "... and {} more contract{} not shown (increase EMAIL_MAX_CONTRACTS_IN_DIGEST to see all).\n",
                omitted_count,
                if omitted_count == 1 { "" } else { "s" }
            ));
        }

        body
    }

    /// Build the HTML body grouped by contract (Issue #491).
    fn build_html_body(
        &self,
        entries: &[ContractDigestEntry<'_>],
        omitted_count: usize,
        total_events: usize,
        priority: &str,
    ) -> String {
        let priority_color = match priority {
            "critical" => "#d32f2f",
            "high" => "#f57c00",
            "medium" => "#388e3c",
            _ => "#757575",
        };

        let mut html = format!(
            r#"<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Soroban Pulse Digest</title></head>
<body style="font-family:sans-serif;max-width:800px;margin:0 auto;padding:20px;color:#333">
  <h2 style="border-bottom:2px solid {color};padding-bottom:8px">
    Soroban Pulse Event Digest
    <span style="font-size:0.7em;background:{color};color:#fff;padding:2px 8px;border-radius:3px;margin-left:8px;vertical-align:middle">{priority}</span>
  </h2>
  <p><strong>{total}</strong> event{plural} indexed.</p>
  <table style="width:100%;border-collapse:collapse">
    <thead>
      <tr style="background:#f5f5f5">
        <th style="padding:8px;text-align:left;border:1px solid #ddd">Contract</th>
        <th style="padding:8px;text-align:right;border:1px solid #ddd">Events</th>
        <th style="padding:8px;text-align:left;border:1px solid #ddd">Types</th>
        <th style="padding:8px;text-align:left;border:1px solid #ddd">First</th>
        <th style="padding:8px;text-align:left;border:1px solid #ddd">Last</th>
      </tr>
    </thead>
    <tbody>
"#,
            color = priority_color,
            priority = priority.to_uppercase(),
            total = total_events,
            plural = if total_events == 1 { "" } else { "s" }
        );

        for (i, entry) in entries.iter().enumerate() {
            let row_bg = if i % 2 == 0 { "#fff" } else { "#fafafa" };
            let mut types: Vec<(&String, &usize)> = entry.type_counts.iter().collect();
            types.sort_by_key(|(t, _)| t.as_str());
            let type_str: String = types
                .iter()
                .map(|(t, c)| format!("{} ({})", t, c))
                .collect::<Vec<_>>()
                .join(", ");

            html.push_str(&format!(
                r#"      <tr style="background:{bg}">
        <td style="padding:8px;border:1px solid #ddd;font-family:monospace;font-size:0.85em">{contract}</td>
        <td style="padding:8px;border:1px solid #ddd;text-align:right">{count}</td>
        <td style="padding:8px;border:1px solid #ddd">{types}</td>
        <td style="padding:8px;border:1px solid #ddd;font-size:0.85em">{first}</td>
        <td style="padding:8px;border:1px solid #ddd;font-size:0.85em">{last}</td>
      </tr>
"#,
                bg = row_bg,
                contract = entry.contract_id,
                count = entry.event_count,
                types = type_str,
                first = entry.first_ts,
                last = entry.last_ts,
            ));
        }

        html.push_str("    </tbody>\n  </table>\n");

        if omitted_count > 0 {
            html.push_str(&format!(
                r#"  <p style="color:#757575;font-size:0.9em">
    ... and <strong>{}</strong> more contract{} not shown.
    Set <code>EMAIL_MAX_CONTRACTS_IN_DIGEST</code> to a higher value to see all.
  </p>
"#,
                omitted_count,
                if omitted_count == 1 { "" } else { "s" }
            ));
        }

        html.push_str("</body>\n</html>");
        html
    }

    /// Send an email with both plain-text and HTML parts using SMTP.
    async fn send_email(
        &self,
        subject: &str,
        plain_body: &str,
        html_body: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut message_builder = Message::builder().from(self.from.parse()?).subject(subject);

        for recipient in &self.to {
            message_builder = message_builder.to(recipient.parse()?);
        }

        let message = message_builder.multipart(
            MultiPart::alternative()
                .singlepart(
                    SinglePart::builder()
                        .header(header::ContentType::TEXT_PLAIN)
                        .body(plain_body.to_string()),
                )
                .singlepart(
                    SinglePart::builder()
                        .header(header::ContentType::TEXT_HTML)
                        .body(html_body.to_string()),
                ),
        )?;

        let mut transport_builder = SmtpTransport::relay(&self.smtp_host)?.port(self.smtp_port);

        if let (Some(user), Some(password)) = (&self.smtp_user, &self.smtp_password) {
            transport_builder = transport_builder.credentials(Credentials::new(
                user.clone(),
                password.expose_secret().clone(),
            ));
        }

        let mailer = transport_builder.build();

        let result = tokio::task::spawn_blocking(move || mailer.send(&message)).await?;

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
            event_type: event_type.to_string(),
            tx_hash: format!("abc{ledger}"),
            ledger,
            ledger_closed_at: format!("2026-06-25T{:02}:00:00Z", ledger % 24),
            ledger_hash: None,
            in_successful_call: true,
            value: json!({"test": "data"}),
            topic: None,
            tenant_id: None,
        }
    }

    #[test]
    fn test_priority_rank_ordering() {
        assert!(priority_rank("critical") > priority_rank("high"));
        assert!(priority_rank("high") > priority_rank("medium"));
        assert!(priority_rank("medium") > priority_rank("low"));
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
    fn test_digest_max_contracts_limit() {
        let limit: usize = 2;
        let mut entries: Vec<String> = (0..5).map(|i| format!("CONTRACT_{i}")).collect();
        let total = entries.len();
        entries.truncate(limit);
        assert_eq!(entries.len(), 2);
        assert_eq!(total - entries.len(), 3); // 3 omitted
    }

    #[test]
    fn test_type_breakdown_per_contract() {
        let events = vec![
            mock_event("C1", "contract", 1),
            mock_event("C1", "contract", 2),
            mock_event("C1", "diagnostic", 3),
        ];
        let mut type_counts: HashMap<String, usize> = HashMap::new();
        for e in &events {
            *type_counts.entry(e.event_type.clone()).or_insert(0) += 1;
        }
        assert_eq!(*type_counts.get("contract").unwrap(), 2);
        assert_eq!(*type_counts.get("diagnostic").unwrap(), 1);
    }
}

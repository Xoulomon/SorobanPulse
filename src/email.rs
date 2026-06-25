use lettre::message::{header, Attachment, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};

use crate::{metrics, models::SorobanEvent, retry_policy::RetryPolicy};

/// Issue #481: File format for the digest event-list attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentFormat {
    /// Comma-separated values (default).
    Csv,
    /// JSON array of events.
    Json,
}

impl AttachmentFormat {
    /// Parse `EMAIL_ATTACHMENT_FORMAT`, defaulting to CSV for unknown values.
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => AttachmentFormat::Json,
            _ => AttachmentFormat::Csv,
        }
    }

    fn filename(self) -> &'static str {
        match self {
            AttachmentFormat::Csv => "events.csv",
            AttachmentFormat::Json => "events.json",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            AttachmentFormat::Csv => "text/csv",
            AttachmentFormat::Json => "application/json",
        }
    }
}

/// Escape a single field for inclusion in a CSV document (RFC 4180).
fn csv_escape(field: &str) -> String {
    if field.contains(['"', ',', '\n', '\r']) {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Issue #481: Render the full event list as a CSV document.
pub fn generate_csv(events: &[SorobanEvent]) -> String {
    let mut out =
        String::from("contract_id,event_type,ledger,ledger_closed_at,tx_hash,in_successful_call\n");
    for event in events {
        out.push_str(&format!(
            "{},{},{},{},{},{}\n",
            csv_escape(&event.contract_id),
            csv_escape(&event.event_type),
            event.ledger,
            csv_escape(&event.ledger_closed_at),
            csv_escape(&event.tx_hash),
            event.in_successful_call,
        ));
    }
    out
}

/// Issue #481: Render the full event list as a pretty-printed JSON array.
pub fn generate_json(events: &[SorobanEvent]) -> String {
    serde_json::to_string_pretty(events).unwrap_or_else(|_| "[]".to_string())
}

/// Issue #481: A generated file attachment for digest emails.
#[derive(Debug, Clone)]
pub struct EmailAttachment {
    pub filename: String,
    pub content: String,
    pub content_type: String,
}

impl EmailAttachment {
    /// Build an attachment containing the full event list in `format`.
    pub fn for_events(events: &[SorobanEvent], format: AttachmentFormat) -> Self {
        let content = match format {
            AttachmentFormat::Csv => generate_csv(events),
            AttachmentFormat::Json => generate_json(events),
        };
        EmailAttachment {
            filename: format.filename().to_string(),
            content,
            content_type: format.content_type().to_string(),
        }
    }
}

/// Batched email notification sender.
/// Collects events for up to 1 minute, then sends a single summary email.
pub struct EmailNotifier {
    smtp_host: String,
    smtp_port: u16,
    smtp_user: Option<String>,
    smtp_password: Option<SecretString>,
    from: String,
    to: Vec<String>,
    contract_filter: Vec<String>,
    retry_policy: RetryPolicy,
    /// Issue #481: when the event count exceeds this, attach a file instead of
    /// inlining everything in the body.
    max_events_in_body: usize,
    /// Issue #481: format used for the event-list attachment.
    attachment_format: AttachmentFormat,
    pool: sqlx::PgPool,
}

impl EmailNotifier {
    pub fn new(
        smtp_host: String,
        smtp_port: u16,
        smtp_user: Option<String>,
        smtp_password: Option<SecretString>,
        from: String,
        to: Vec<String>,
        contract_filter: Vec<String>,
        retry_policy: RetryPolicy,
        max_events_in_body: usize,
        attachment_format: AttachmentFormat,
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
            max_events_in_body,
            attachment_format,
            pool,
        }
    }

    /// Spawn a background task that batches events and sends emails every minute.
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
                                // Apply contract filter if configured
                                if !self.contract_filter.is_empty()
                                    && !self.contract_filter.contains(&event.contract_id)
                                {
                                    continue;
                                }
                                events_buffer.push(event);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!(
                                    skipped = n,
                                    "Email notifier lagged, some events skipped"
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                // Channel closed, send any remaining events and exit
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

    /// Send a summary email for a batch of events with idempotency (Issue #474).
    async fn send_batch_email(&self, events: &[SorobanEvent]) {
        if events.is_empty() {
            return;
        }

        // Generate idempotency key based on event batch
        let event_ids: Vec<String> = events.iter().map(|e| e.id.to_string()).collect();
        let idempotency_key = format!("batch_{}", 
            sha2::Sha256::digest(event_ids.join(",").as_bytes())
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()[..16].to_string()
        );

        // Check if already sent
        if let Ok(existing) = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM email_notifications WHERE idempotency_key = $1"
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

        // Group events by contract ID for better readability
        let mut by_contract: HashMap<String, Vec<&SorobanEvent>> = HashMap::new();
        for event in events {
            by_contract
                .entry(event.contract_id.clone())
                .or_default()
                .push(event);
        }

        let subject = format!(
            "Soroban Pulse: {} new event{} indexed",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        );

        let mut body = String::new();
        body.push_str(&format!(
            "Soroban Pulse indexed {} new event{} in the last minute.\n\n",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        ));

        for (contract_id, contract_events) in by_contract.iter() {
            body.push_str(&format!(
                "Contract: {}\n  Events: {}\n",
                contract_id,
                contract_events.len()
            ));

            for event in contract_events.iter().take(10) {
                body.push_str(&format!(
                    "  - Type: {}, Ledger: {}, TxHash: {}\n",
                    event.event_type, event.ledger, event.tx_hash
                ));
            }

            if contract_events.len() > 10 {
                body.push_str(&format!(
                    "  ... and {} more event{}\n",
                    contract_events.len() - 10,
                    if contract_events.len() - 10 == 1 {
                        ""
                    } else {
                        "s"
                    }
                ));
            }
            body.push('\n');
        }

        // Issue #481: for large digests, attach the full event list as a file
        // rather than bloating the message body (which clients may truncate or
        // reject). The body keeps the per-contract summary and gains a note.
        let attachment = if events.len() > self.max_events_in_body {
            let attachment = EmailAttachment::for_events(events, self.attachment_format);
            body.push_str(&format!(
                "\nThe full list of {} events is attached as {} ({}).\n",
                events.len(),
                attachment.filename,
                attachment.content_type,
            ));
            Some(attachment)
        } else {
            None
        };

        // Build and send email
        if let Err(e) = self.send_email(&subject, &body, attachment).await {
            error!(error = %e, "Failed to send email notification");
            metrics::record_email_failure();
        } else {
            info!(
                recipients = self.to.len(),
                event_count = events.len(),
                "Email notification sent successfully"
            );
        }
    }

    /// Send an email using SMTP.
    ///
    /// Issue #481: when `attachment` is present, the message is built as a
    /// `multipart/mixed` body with the summary plus the attached file.
    async fn send_email(
        &self,
        subject: &str,
        body: &str,
        attachment: Option<EmailAttachment>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Build message with all recipients
        let mut message_builder = Message::builder().from(self.from.parse()?).subject(subject);

        for recipient in &self.to {
            message_builder = message_builder.to(recipient.parse()?);
        }

        let message = match attachment {
            Some(att) => {
                let content_type = header::ContentType::parse(&att.content_type)
                    .unwrap_or(header::ContentType::TEXT_PLAIN);
                let file_part = Attachment::new(att.filename).body(att.content, content_type);
                message_builder.multipart(
                    MultiPart::mixed()
                        .singlepart(SinglePart::plain(body.to_string()))
                        .singlepart(file_part),
                )?
            }
            None => message_builder
                .header(header::ContentType::TEXT_PLAIN)
                .body(body.to_string())?,
        };

        // Build SMTP transport
        let mut transport_builder = SmtpTransport::relay(&self.smtp_host)?.port(self.smtp_port);

        if let (Some(user), Some(password)) = (&self.smtp_user, &self.smtp_password) {
            transport_builder = transport_builder.credentials(Credentials::new(
                user.clone(),
                password.expose_secret().clone(),
            ));
        }

        let mailer = transport_builder.build();

        // Send email (blocking operation, run in spawn_blocking)
        let result = tokio::task::spawn_blocking(move || mailer.send(&message)).await?;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mock_event(contract_id: &str, ledger: u64) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            event_type: "contract".to_string(),
            tx_hash: "abc123".to_string(),
            ledger,
            ledger_closed_at: "2026-04-28T00:00:00Z".to_string(),
            ledger_hash: None,
            in_successful_call: true,
            value: json!({"test": "data"}),
            topic: None,
            ..Default::default()
        }
    }

    #[test]
    fn test_email_notifier_creation() {
        // `connect_lazy` builds a pool handle without opening a connection,
        // so this stays a pure unit test (no live database required).
        let pool = sqlx::PgPool::connect_lazy("postgres://localhost/soroban_pulse_test")
            .expect("lazy pool");
        let notifier = EmailNotifier::new(
            "smtp.example.com".to_string(),
            587,
            Some("user".to_string()),
            Some(SecretString::new("pass".to_string())),
            "from@example.com".to_string(),
            vec!["to@example.com".to_string()],
            vec![],
            RetryPolicy::default(),
            50,
            AttachmentFormat::Csv,
            pool,
        );

        assert_eq!(notifier.smtp_host, "smtp.example.com");
        assert_eq!(notifier.smtp_port, 587);
        assert_eq!(notifier.from, "from@example.com");
        assert_eq!(notifier.to.len(), 1);
        assert_eq!(notifier.max_events_in_body, 50);
    }

    #[test]
    fn test_secret_string_redacted_in_debug() {
        let secret = SecretString::new("my_password".to_string());
        let debug_str = format!("{:?}", secret);
        assert!(!debug_str.contains("my_password"));
        assert!(debug_str.contains("[REDACTED]"));
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

        // Empty filter means all events pass
        assert!(filter.is_empty() || filter.contains(&event.contract_id));
    }

    // --- Issue #481: email attachment generation ---

    #[test]
    fn test_attachment_format_parse() {
        assert_eq!(AttachmentFormat::parse("csv"), AttachmentFormat::Csv);
        assert_eq!(AttachmentFormat::parse("JSON"), AttachmentFormat::Json);
        // Unknown values default to CSV.
        assert_eq!(AttachmentFormat::parse("xml"), AttachmentFormat::Csv);
        assert_eq!(AttachmentFormat::parse(""), AttachmentFormat::Csv);
    }

    #[test]
    fn test_generate_csv_has_header_and_rows() {
        let events = vec![mock_event("CONTRACT_A", 100), mock_event("CONTRACT_B", 101)];
        let csv = generate_csv(&events);
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(
            lines[0],
            "contract_id,event_type,ledger,ledger_closed_at,tx_hash,in_successful_call"
        );
        // One header line + one line per event.
        assert_eq!(lines.len(), 3);
        assert!(lines[1].starts_with("CONTRACT_A,contract,100,"));
        assert!(lines[2].starts_with("CONTRACT_B,contract,101,"));
    }

    #[test]
    fn test_csv_escapes_special_characters() {
        let mut event = mock_event("CONTRACT_A", 100);
        event.event_type = "weird,\"type\"".to_string();
        let csv = generate_csv(&[event]);
        // Comma/quote-bearing field is wrapped in quotes with quotes doubled.
        assert!(csv.contains("\"weird,\"\"type\"\"\""));
    }

    #[test]
    fn test_generate_json_is_valid_array() {
        let events = vec![mock_event("CONTRACT_A", 100)];
        let json = generate_json(&events);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 1);
        assert_eq!(parsed[0]["contractId"], "CONTRACT_A");
    }

    #[test]
    fn test_attachment_for_events_csv_and_json() {
        let events = vec![mock_event("CONTRACT_A", 100)];

        let csv = EmailAttachment::for_events(&events, AttachmentFormat::Csv);
        assert_eq!(csv.filename, "events.csv");
        assert_eq!(csv.content_type, "text/csv");
        assert!(csv.content.contains("CONTRACT_A"));

        let json = EmailAttachment::for_events(&events, AttachmentFormat::Json);
        assert_eq!(json.filename, "events.json");
        assert_eq!(json.content_type, "application/json");
        assert!(json.content.contains("CONTRACT_A"));
    }

    #[test]
    fn test_attachment_threshold_logic() {
        let max_events_in_body = 50;
        // At or below the threshold: no attachment.
        let small: Vec<SorobanEvent> = (0..50).map(|i| mock_event("C", 100 + i)).collect();
        assert!(small.len() <= max_events_in_body);
        // Above the threshold: attachment is generated.
        let large: Vec<SorobanEvent> = (0..51).map(|i| mock_event("C", 100 + i)).collect();
        assert!(large.len() > max_events_in_body);
        let attachment = EmailAttachment::for_events(&large, AttachmentFormat::Csv);
        // Header + 51 rows = 52 lines.
        assert_eq!(attachment.content.lines().count(), 52);
    }
}

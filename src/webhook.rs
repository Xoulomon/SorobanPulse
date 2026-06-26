use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::Sha256;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Validate that `url` is reachable and returns a 2xx response.
/// Used when creating or updating webhook notification channels (#503).
pub async fn validate_webhook_url(url: &str) -> Result<(), String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("failed to build HTTP client: {}", e))?;

    match client.head(url).send().await {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp) => Err(format!(
            "webhook URL returned non-2xx status: {}",
            resp.status()
        )),
        Err(e) => Err(format!("webhook URL is unreachable: {}", e)),
    }
}

use crate::{metrics, models::SorobanEvent};

type HmacSha256 = Hmac<Sha256>;

/// Sign a payload with HMAC-SHA256 and return the hex digest.
pub fn sign_payload(secret: &str, body: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body);
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

/// Evaluate the notification priority of an event using a JSONPath-style rule (Issue #492).
/// Returns the matched priority string, or `default_priority` if no rule is set or matches.
pub fn evaluate_priority<'a>(
    event: &SorobanEvent,
    rule_path: Option<&str>,
    rule_value: Option<&str>,
    rule_priority: Option<&'a str>,
    default_priority: &'a str,
) -> &'a str {
    if let (Some(path), Some(expected), Some(priority)) = (rule_path, rule_value, rule_priority) {
        let segments: Vec<&str> = path
            .trim_start_matches("$.")
            .split('.')
            .filter(|s| !s.is_empty())
            .collect();

        let mut current = &event.value;
        for segment in &segments {
            match current.get(segment) {
                Some(next) => current = next,
                None => return default_priority,
            }
        }

        if current.as_str() == Some(expected) {
            return priority;
        }
    }
    default_priority
}

/// Deliver a single event to the webhook URL with the default retry policy.
/// On final failure, insert into DLQ.
pub async fn deliver(
    client: Client,
    url: String,
    secret: Option<String>,
    event: SorobanEvent,
    pool: Option<&sqlx::PgPool>,
) {
    deliver_with_retry_policy(
        client,
        url,
        secret,
        event,
        pool,
        &crate::retry_policy::RetryPolicy::webhook_default(),
        "medium".to_string(),
    )
    .await
}

/// Deliver with custom retry policy and priority (Issues #474, #492).
/// The priority is included in the request payload and headers.
/// On success, a notification_acknowledgments record is inserted for escalation
/// tracking (Issue #493).
///
/// All parameters are owned so the resulting future is `'static` and safe to
/// pass to `tokio::spawn`.
pub async fn deliver_with_retry_policy(
    client: Client,
    url: String,
    secret: Option<String>,
    event: SorobanEvent,
    pool: Option<&sqlx::PgPool>,
    retry_policy: &crate::retry_policy::RetryPolicy,
    priority: String,
) {
    // Check suppression list before attempting delivery (Issue #490)
    if let Some(pool) = pool {
        match sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM suppression_lists \
             WHERE target = $1 AND target_type = 'webhook' \
             AND (expires_at IS NULL OR expires_at > NOW())",
        )
        .bind(&url)
        .fetch_one(pool)
        .await
        {
            Ok(count) if count > 0 => {
                info!(url = %url, "Webhook URL suppressed, skipping delivery");
                crate::metrics::record_notification_suppressed();
                return;
            }
            _ => {}
        }
    }

    let body = match serde_json::to_vec(&event) {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "Failed to serialize event for webhook delivery");
            return;
        }
    };
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("priority".to_string(), serde_json::json!(priority));
    }

    let body = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "Failed to re-serialize webhook payload");
            return;
        }
    };

    let signature = secret.as_deref().map(|s| sign_payload(s, &body));
    let priority_owned = priority.clone();

    let result = retry_policy
        .execute_with_retry(|attempt| {
            let client = client.clone();
            let url = url.clone();
            let body = body.clone();
            let signature = signature.clone();
            let priority_header = priority_owned.clone();

            async move {
                let mut req = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .header("X-Notification-Priority", priority_header)
                    .body(body);

                if let Some(ref sig) = signature {
                    req = req.header("X-Signature-256", format!("sha256={sig}"));
                }

                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        info!(
                            url = %url,
                            contract_id = %event.contract_id,
                            attempt = attempt,
                            priority = %event.event_type,
                            "Webhook delivered successfully"
                        );
                        Ok(())
                    }
                    Ok(resp) => {
                        let error_msg = format!(
                            "HTTP {}: {}",
                            resp.status(),
                            resp.text().await.unwrap_or_default()
                        );
                        Err(error_msg)
                    }
                    Err(e) => Err(format!("Request error: {}", e)),
                }
            }
        })
        .await;

    match result {
        Ok(()) => {
            // Record the notification for escalation tracking (Issue #493).
            if let Some(pool) = pool {
                let notification_id = Uuid::new_v4();
                if let Err(e) = sqlx::query(
                    "INSERT INTO notification_acknowledgments \
                     (id, channel, event_contract_id, event_type, priority, status) \
                     VALUES ($1, 'webhook', $2, $3, $4, 'pending')",
                )
                .bind(notification_id)
                .bind(&event.contract_id)
                .bind(&event.event_type)
                .bind(priority)
                .execute(pool)
                .await
                {
                    warn!(error = %e, "Failed to record notification for escalation tracking");
                }
            }
        }
        Err(error_msg) => {
            error!(
                url = %url,
                contract_id = %event.contract_id,
                error = %error_msg,
                max_attempts = retry_policy.max_attempts,
                "Webhook delivery failed after all retries"
            );

            if let Some(pool) = pool {
                let payload_val = serde_json::to_value(&event).unwrap_or(serde_json::json!({}));
                let next_retry = chrono::Utc::now() + chrono::Duration::seconds(60);

                if let Err(e) = sqlx::query(
                    "INSERT INTO webhook_failures \
                     (url, payload, attempts, last_error, next_retry_at) \
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(&url)
                .bind(payload_val)
                .bind(retry_policy.max_attempts as i32)
                .bind(&error_msg)
                .bind(next_retry)
                .execute(pool)
                .await
                {
                    error!(error = %e, "Failed to insert webhook failure into DLQ");
                }
            }

            metrics::record_webhook_failure();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mock_event(contract_id: &str) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            event_type: "contract".to_string(),
            tx_hash: "abc123".to_string(),
            ledger: 100,
            ledger_closed_at: "2026-06-25T00:00:00Z".to_string(),
            ledger_hash: None,
            in_successful_call: true,
            value: json!({"amount": "100", "action": "transfer"}),
            topic: None,
            tenant_id: None,
        }
    }

    #[test]
    fn test_sign_payload_produces_consistent_hex() {
        let sig1 = sign_payload("mysecret", b"hello world");
        let sig2 = sign_payload("mysecret", b"hello world");
        assert_eq!(sig1, sig2);
        assert_eq!(sig1.len(), 64);
    }

    #[test]
    fn test_sign_payload_different_secrets_differ() {
        let sig1 = sign_payload("secret1", b"payload");
        let sig2 = sign_payload("secret2", b"payload");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_sign_payload_known_value() {
        let sig = sign_payload("key", b"test");
        assert_eq!(
            sig,
            "02afb56304902c656fcb737cdd03de6205bb6d401da2812efd9b2d36a08af159"
        );
    }

    #[test]
    fn test_evaluate_priority_no_rule_returns_default() {
        let event = mock_event("C1");
        let p = evaluate_priority(&event, None, None, None, "medium");
        assert_eq!(p, "medium");
    }

    #[test]
    fn test_evaluate_priority_rule_matches() {
        let event = mock_event("C1");
        // event.value = {"amount": "100", "action": "transfer"}
        let p = evaluate_priority(
            &event,
            Some("$.action"),
            Some("transfer"),
            Some("critical"),
            "medium",
        );
        assert_eq!(p, "critical");
    }

    #[test]
    fn test_evaluate_priority_rule_no_match_returns_default() {
        let event = mock_event("C1");
        let p = evaluate_priority(
            &event,
            Some("$.action"),
            Some("mint"),
            Some("critical"),
            "low",
        );
        assert_eq!(p, "low");
    }

    #[test]
    fn test_evaluate_priority_missing_path_returns_default() {
        let event = mock_event("C1");
        let p = evaluate_priority(
            &event,
            Some("$.nonexistent.nested"),
            Some("value"),
            Some("high"),
            "medium",
        );
        assert_eq!(p, "medium");
    }
}

//! Notification deduplication (Issue #478).
//!
//! The notification system delivers notifications for each event as it is
//! indexed. If the indexer re-processes a ledger range (e.g. after a restart
//! with a stale checkpoint), the same events may be indexed again. While the
//! `ON CONFLICT DO NOTHING` constraint prevents duplicate DB inserts, the
//! notification system could still deliver duplicate notifications.
//!
//! To prevent this we track an `events.notified_at` timestamp. Before
//! delivering a notification we check whether the event has already been
//! notified, and we set `notified_at` after a successful delivery.

use crate::{metrics, models::SorobanEvent};

/// Returns `true` if a notification has already been sent for this event,
/// i.e. the matching `events` row has a non-null `notified_at`.
pub async fn already_notified(
    pool: &sqlx::PgPool,
    event: &SorobanEvent,
) -> Result<bool, sqlx::Error> {
    let notified: Option<bool> = sqlx::query_scalar(
        "SELECT notified_at IS NOT NULL \
         FROM events \
         WHERE tx_hash = $1 AND contract_id = $2 AND event_type = $3 \
         LIMIT 1",
    )
    .bind(&event.tx_hash)
    .bind(&event.contract_id)
    .bind(&event.event_type)
    .fetch_optional(pool)
    .await?;

    Ok(notified.unwrap_or(false))
}

/// Mark an event as notified by setting `notified_at` to `NOW()`.
///
/// Only updates rows where `notified_at` is still NULL so repeated calls are
/// idempotent and never move the timestamp forward.
pub async fn mark_notified(
    pool: &sqlx::PgPool,
    event: &SorobanEvent,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE events SET notified_at = NOW() \
         WHERE tx_hash = $1 AND contract_id = $2 AND event_type = $3 \
         AND notified_at IS NULL",
    )
    .bind(&event.tx_hash)
    .bind(&event.contract_id)
    .bind(&event.event_type)
    .execute(pool)
    .await?;

    Ok(())
}

/// Decide whether a notification for `event` should be delivered.
///
/// Returns `false` (and increments the deduplication counter) when the event
/// was already notified. On a database error we fail open and deliver, so a
/// transient DB issue never silently drops a notification.
pub async fn should_deliver(pool: &sqlx::PgPool, event: &SorobanEvent) -> bool {
    match already_notified(pool, event).await {
        Ok(true) => {
            metrics::record_notification_deduplicated();
            tracing::debug!(
                contract_id = %event.contract_id,
                tx_hash = %event.tx_hash,
                event_type = %event.event_type,
                "Skipping duplicate notification (already notified)"
            );
            false
        }
        Ok(false) => true,
        Err(e) => {
            tracing::warn!(error = %e, "Notification dedup check failed; delivering anyway");
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sqlx::PgPool;

    fn mock_event(tx_hash: &str, contract_id: &str, event_type: &str) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            event_type: event_type.to_string(),
            tx_hash: tx_hash.to_string(),
            ledger: 100,
            ledger_closed_at: "2026-06-25T00:00:00Z".to_string(),
            ledger_hash: None,
            in_successful_call: true,
            value: json!({"amount": "1"}),
            topic: None,
            tenant_id: None,
        }
    }

    async fn insert_event(pool: &PgPool, event: &SorobanEvent) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) \
             VALUES ($1, $2, $3, $4, NOW(), $5) \
             ON CONFLICT DO NOTHING",
        )
        .bind(&event.contract_id)
        .bind(&event.event_type)
        .bind(&event.tx_hash)
        .bind(event.ledger as i64)
        .bind(&event.value)
        .execute(pool)
        .await
        .expect("insert event");
    }

    #[sqlx::test]
    async fn fresh_event_is_not_yet_notified(pool: PgPool) {
        let event = mock_event("tx_fresh", "CONTRACT_A", "contract");
        insert_event(&pool, &event).await;

        assert!(!already_notified(&pool, &event).await.unwrap());
        assert!(should_deliver(&pool, &event).await);
    }

    #[sqlx::test]
    async fn marking_notified_makes_event_deduplicated(pool: PgPool) {
        let event = mock_event("tx_dup", "CONTRACT_A", "contract");
        insert_event(&pool, &event).await;

        mark_notified(&pool, &event).await.unwrap();

        assert!(already_notified(&pool, &event).await.unwrap());
        // A re-indexed (duplicate) event must not be delivered again.
        assert!(!should_deliver(&pool, &event).await);
    }

    #[sqlx::test]
    async fn mark_notified_is_idempotent(pool: PgPool) {
        let event = mock_event("tx_idem", "CONTRACT_A", "contract");
        insert_event(&pool, &event).await;

        mark_notified(&pool, &event).await.unwrap();
        let first: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT notified_at FROM events WHERE tx_hash = $1 AND contract_id = $2 AND event_type = $3",
        )
        .bind(&event.tx_hash)
        .bind(&event.contract_id)
        .bind(&event.event_type)
        .fetch_one(&pool)
        .await
        .unwrap();

        // Second call must not move the timestamp forward.
        mark_notified(&pool, &event).await.unwrap();
        let second: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT notified_at FROM events WHERE tx_hash = $1 AND contract_id = $2 AND event_type = $3",
        )
        .bind(&event.tx_hash)
        .bind(&event.contract_id)
        .bind(&event.event_type)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(first, second);
    }
}

-- Issue #478: notification deduplication
-- Track when a notification was sent for an event so that re-indexing a
-- ledger range does not deliver duplicate notifications for already-notified
-- events.
ALTER TABLE events ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ;

-- Partial index keeps lookups for un-notified events cheap.
CREATE INDEX IF NOT EXISTS idx_events_notified_at
    ON events (tx_hash, contract_id, event_type)
    WHERE notified_at IS NOT NULL;

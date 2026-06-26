DROP INDEX IF EXISTS idx_events_notified_at;
ALTER TABLE events DROP COLUMN IF EXISTS notified_at;

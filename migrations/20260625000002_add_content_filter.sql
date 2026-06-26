-- Issue #477: content-based notification filtering.
--
-- Adds an optional content_filter to notification channels. The filter is a
-- JSONB object of the form {"path": "$.amount", "op": "gt", "value": "1000000"}
-- and is evaluated against an event's data before delivery; only matching events
-- are delivered through the channel.
ALTER TABLE notification_channels
    ADD COLUMN IF NOT EXISTS content_filter JSONB;

-- #510 search/filter, #509 tags, #508 owner/status, for notification channels
ALTER TABLE notification_channels
    ADD COLUMN IF NOT EXISTS description TEXT,
    ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS owner TEXT,
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'inactive', 'paused')),
    ADD COLUMN IF NOT EXISTS contract_filter TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_notification_channels_fts
    ON notification_channels
    USING gin(to_tsvector('english', name || ' ' || coalesce(description, '')));

CREATE INDEX IF NOT EXISTS idx_notification_channels_tags
    ON notification_channels USING gin(tags);

CREATE INDEX IF NOT EXISTS idx_notification_channels_channel_type
    ON notification_channels(channel_type);

CREATE INDEX IF NOT EXISTS idx_notification_channels_status
    ON notification_channels(status);

CREATE INDEX IF NOT EXISTS idx_notification_channels_owner
    ON notification_channels(owner);

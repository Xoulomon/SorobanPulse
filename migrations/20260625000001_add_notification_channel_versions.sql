-- Migration for Issue #505: Notification channel versioning
CREATE TABLE IF NOT EXISTS notification_channel_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id UUID NOT NULL REFERENCES notification_channels(id) ON DELETE CASCADE,
    version_number INTEGER NOT NULL,
    name TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    config JSONB NOT NULL,
    retry_policy JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (channel_id, version_number)
);

CREATE INDEX IF NOT EXISTS idx_ncv_channel_id ON notification_channel_versions(channel_id);

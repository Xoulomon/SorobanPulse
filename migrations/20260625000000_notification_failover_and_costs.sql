-- Issue #499: Add failover_channel field to notification_channels
ALTER TABLE notification_channels
    ADD COLUMN IF NOT EXISTS failover_channel_id UUID REFERENCES notification_channels(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS cost_per_notification_cents INTEGER NOT NULL DEFAULT 0;

-- Issue #501: Track cumulative notification costs
CREATE TABLE IF NOT EXISTS notification_costs (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id     UUID        NOT NULL REFERENCES notification_channels(id) ON DELETE CASCADE,
    channel_name   TEXT        NOT NULL,
    channel_type   TEXT        NOT NULL,
    cost_cents     INTEGER     NOT NULL DEFAULT 0,
    sent_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notification_costs_channel_id ON notification_costs(channel_id);
CREATE INDEX IF NOT EXISTS idx_notification_costs_sent_at    ON notification_costs(sent_at);

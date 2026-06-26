-- #507 notification channel groups
CREATE TABLE IF NOT EXISTS notification_channel_groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notification_channel_group_members (
    group_id UUID NOT NULL REFERENCES notification_channel_groups(id) ON DELETE CASCADE,
    channel_id UUID NOT NULL REFERENCES notification_channels(id) ON DELETE CASCADE,
    PRIMARY KEY (group_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_ncgm_group_id
    ON notification_channel_group_members(group_id);

CREATE INDEX IF NOT EXISTS idx_ncgm_channel_id
    ON notification_channel_group_members(channel_id);

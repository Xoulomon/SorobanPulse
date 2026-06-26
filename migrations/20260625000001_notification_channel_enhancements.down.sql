DROP INDEX IF EXISTS idx_notification_channels_owner;
DROP INDEX IF EXISTS idx_notification_channels_status;
DROP INDEX IF EXISTS idx_notification_channels_channel_type;
DROP INDEX IF EXISTS idx_notification_channels_tags;
DROP INDEX IF EXISTS idx_notification_channels_fts;

ALTER TABLE notification_channels
    DROP COLUMN IF EXISTS contract_filter,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS owner,
    DROP COLUMN IF EXISTS tags,
    DROP COLUMN IF EXISTS description;

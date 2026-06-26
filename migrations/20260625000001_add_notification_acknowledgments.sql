-- Issue #493: Add notification_acknowledgments table for escalation policy support.
-- Notifications delivered via webhook or email are recorded here. A background
-- task escalates any notification whose status remains 'pending' for longer than
-- escalation_delay_minutes.

CREATE TABLE IF NOT EXISTS notification_acknowledgments (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    channel             VARCHAR(50) NOT NULL,               -- 'webhook' | 'email' | 'sms'
    event_contract_id   VARCHAR(100),
    event_type          VARCHAR(50),
    priority            VARCHAR(20) NOT NULL DEFAULT 'medium',
    status              VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending' | 'acknowledged' | 'escalated'
    escalation_channel  VARCHAR(100),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at     TIMESTAMPTZ,
    escalated_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_notif_ack_status_created
    ON notification_acknowledgments (status, created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_notif_ack_channel
    ON notification_acknowledgments (channel);

COMMENT ON TABLE notification_acknowledgments IS
    'Tracks delivered notifications and their acknowledgment state for escalation policy (Issue #493).';

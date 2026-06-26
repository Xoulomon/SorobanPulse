-- Email deliveries table for A/B test tracking (Issue #489)
CREATE TABLE IF NOT EXISTS email_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email_notification_id UUID REFERENCES email_notifications(id) ON DELETE CASCADE,
    recipient TEXT NOT NULL,
    ab_template CHAR(1),
    delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_email_deliveries_recipient ON email_deliveries(recipient);
CREATE INDEX IF NOT EXISTS idx_email_deliveries_ab_template ON email_deliveries(ab_template);
CREATE INDEX IF NOT EXISTS idx_email_deliveries_delivered_at ON email_deliveries(delivered_at);

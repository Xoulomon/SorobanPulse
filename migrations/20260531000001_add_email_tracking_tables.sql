-- Email open tracking table (Issue #487)
CREATE TABLE IF NOT EXISTS email_opens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token TEXT NOT NULL UNIQUE,
    email_notification_id UUID REFERENCES email_notifications(id) ON DELETE CASCADE,
    recipient TEXT NOT NULL,
    opened_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_email_opens_token ON email_opens(token);
CREATE INDEX IF NOT EXISTS idx_email_opens_recipient ON email_opens(recipient);
CREATE INDEX IF NOT EXISTS idx_email_opens_opened_at ON email_opens(opened_at);

-- Email click tracking table (Issue #488)
CREATE TABLE IF NOT EXISTS email_clicks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token TEXT NOT NULL UNIQUE,
    email_notification_id UUID REFERENCES email_notifications(id) ON DELETE CASCADE,
    recipient TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    clicked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_email_clicks_token ON email_clicks(token);
CREATE INDEX IF NOT EXISTS idx_email_clicks_recipient ON email_clicks(recipient);
CREATE INDEX IF NOT EXISTS idx_email_clicks_clicked_at ON email_clicks(clicked_at);

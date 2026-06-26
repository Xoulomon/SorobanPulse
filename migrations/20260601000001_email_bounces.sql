-- Migration for Issue #484: email bounce handling.
-- Stores addresses that have bounced so notification emails are no longer sent
-- to them, protecting the sender's reputation and saving SMTP resources.
CREATE TABLE IF NOT EXISTS email_bounces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    reason TEXT,
    provider TEXT,
    bounced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_email_bounces_email ON email_bounces(email);

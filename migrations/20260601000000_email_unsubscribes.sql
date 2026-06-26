-- Migration for Issue #483: email unsubscribe links (CAN-SPAM / GDPR)
-- Stores a unique, per-recipient unsubscribe token. When unsubscribed_at is
-- non-NULL the recipient has opted out and no further emails are sent.
CREATE TABLE IF NOT EXISTS email_unsubscribes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    token TEXT NOT NULL UNIQUE,
    unsubscribed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_email_unsubscribes_token ON email_unsubscribes(token);
CREATE INDEX IF NOT EXISTS idx_email_unsubscribes_email ON email_unsubscribes(email);

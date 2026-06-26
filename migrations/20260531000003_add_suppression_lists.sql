-- Suppression lists for email and webhook notifications (Issue #490)
CREATE TABLE IF NOT EXISTS suppression_lists (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target TEXT NOT NULL,
    target_type TEXT NOT NULL CHECK (target_type IN ('email', 'webhook')),
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_suppression_lists_target ON suppression_lists(target, target_type);
CREATE INDEX IF NOT EXISTS idx_suppression_lists_type ON suppression_lists(target_type);
CREATE INDEX IF NOT EXISTS idx_suppression_lists_expires ON suppression_lists(expires_at);

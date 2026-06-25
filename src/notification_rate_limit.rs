//! Per-channel notification rate limiting (Issue #476).
//!
//! The notification system delivers every matching event to every configured
//! channel. During a high-volume event burst this can send thousands of
//! notifications per minute to a single channel, overwhelming the recipient and
//! tripping provider rate limits (SendGrid, SES, webhook endpoints).
//!
//! Each channel gets a token-bucket rate limiter (built on the `governor`
//! crate). When the limit is exceeded, the surplus notifications are *batched*:
//! delivery awaits [`ChannelRateLimiter::acquire`], which blocks until a token
//! frees up after the window resets, rather than dropping the notification.

use std::num::NonZeroU32;
use std::sync::Arc;

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};

use crate::metrics;

/// Per-channel rate limit configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateLimitConfig {
    /// Maximum notifications per minute. `None` disables the per-minute limit.
    pub max_per_minute: Option<u32>,
    /// Maximum notifications per hour. `None` disables the per-hour limit.
    pub max_per_hour: Option<u32>,
}

impl RateLimitConfig {
    pub fn new(max_per_minute: Option<u32>, max_per_hour: Option<u32>) -> Self {
        Self {
            max_per_minute,
            max_per_hour,
        }
    }

    /// True when at least one limit is configured.
    pub fn is_enabled(&self) -> bool {
        self.max_per_minute.is_some() || self.max_per_hour.is_some()
    }
}

/// A token-bucket rate limiter for a single notification channel.
///
/// Holds up to two limiters — one for the per-minute quota and one for the
/// per-hour quota — and only admits a notification when *both* allow it.
pub struct ChannelRateLimiter {
    per_minute: Option<DefaultDirectRateLimiter>,
    per_hour: Option<DefaultDirectRateLimiter>,
}

impl ChannelRateLimiter {
    /// Build a limiter from config. Returns `None` when no limit is configured,
    /// so callers can cheaply skip rate-limiting entirely.
    pub fn from_config(config: &RateLimitConfig) -> Option<Arc<Self>> {
        if !config.is_enabled() {
            return None;
        }

        let per_minute = config
            .max_per_minute
            .and_then(NonZeroU32::new)
            .map(|n| RateLimiter::direct(Quota::per_minute(n)));
        let per_hour = config
            .max_per_hour
            .and_then(NonZeroU32::new)
            .map(|n| RateLimiter::direct(Quota::per_hour(n)));

        Some(Arc::new(Self {
            per_minute,
            per_hour,
        }))
    }

    /// Non-blocking check: consume a token from each configured limiter and
    /// return `true` if the notification may be sent immediately. Used in tests
    /// and for callers that prefer to make their own batching decision.
    pub fn check(&self) -> bool {
        let minute_ok = self.per_minute.as_ref().map(|l| l.check().is_ok()).unwrap_or(true);
        let hour_ok = self.per_hour.as_ref().map(|l| l.check().is_ok()).unwrap_or(true);
        minute_ok && hour_ok
    }

    /// Acquire permission to deliver one notification, waiting for the rate
    /// limit window to reset if necessary. This is what implements batching:
    /// surplus notifications queue up here instead of being dropped.
    ///
    /// Returns `true` if the call had to wait (i.e. the notification was
    /// rate-limited and batched), in which case the
    /// `soroban_pulse_notification_rate_limited_total` counter is incremented.
    pub async fn acquire(&self) -> bool {
        let mut rate_limited = false;

        if let Some(limiter) = &self.per_minute {
            if limiter.check().is_err() {
                rate_limited = true;
                limiter.until_ready().await;
            }
        }
        if let Some(limiter) = &self.per_hour {
            if limiter.check().is_err() {
                rate_limited = true;
                limiter.until_ready().await;
            }
        }

        if rate_limited {
            metrics::record_notification_rate_limited();
        }
        rate_limited
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_config_builds_no_limiter() {
        let cfg = RateLimitConfig::default();
        assert!(!cfg.is_enabled());
        assert!(ChannelRateLimiter::from_config(&cfg).is_none());
    }

    #[test]
    fn per_minute_limit_admits_up_to_quota_then_blocks() {
        let cfg = RateLimitConfig::new(Some(3), None);
        let limiter = ChannelRateLimiter::from_config(&cfg).expect("limiter");

        // The token bucket starts full: the first `max_per_minute` checks pass.
        assert!(limiter.check());
        assert!(limiter.check());
        assert!(limiter.check());
        // The next one is over budget and must be batched.
        assert!(!limiter.check());
    }

    #[test]
    fn both_limits_must_allow() {
        // Per-minute allows 5 but per-hour only allows 1.
        let cfg = RateLimitConfig::new(Some(5), Some(1));
        let limiter = ChannelRateLimiter::from_config(&cfg).expect("limiter");

        assert!(limiter.check());
        // Second call: per-minute still has budget but per-hour is exhausted.
        assert!(!limiter.check());
    }

    #[tokio::test]
    async fn acquire_reports_rate_limited_when_over_budget() {
        let cfg = RateLimitConfig::new(Some(1), None);
        let limiter = ChannelRateLimiter::from_config(&cfg).expect("limiter");

        // First acquire is immediate.
        assert!(!limiter.acquire().await);
        // Second is over budget: it will wait for the window and report limited.
        assert!(limiter.acquire().await);
    }
}

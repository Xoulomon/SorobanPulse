# Per-Channel Notification Rate Limiting

_Issue #476_

The notification system delivers every matching event to every configured
channel. During a high-volume event burst (for example a popular contract
launch) this can send thousands of notifications per minute to a single channel,
overwhelming the recipient and tripping provider rate limits (SendGrid, SES, or
the webhook endpoint itself), which can cause delivery failures or account
suspension.

To prevent this, each channel has its own **token-bucket rate limiter** built on
the [`governor`](https://docs.rs/governor) crate.

## Behaviour

- Each channel is limited to a configurable number of notifications per minute
  and/or per hour.
- The bucket starts full, so short bursts up to the configured limit are
  delivered immediately.
- When the limit is exceeded, the surplus notifications are **batched**: delivery
  waits until a token frees up after the rate-limit window resets, rather than
  dropping the notification.
- Both the per-minute and per-hour limits must allow a notification before it is
  delivered.

## Configuration

Rate limits are configured per channel. For the webhook channel:

| Environment variable            | Description                                          |
| ------------------------------- | ---------------------------------------------------- |
| `WEBHOOK_RATE_LIMIT_PER_MINUTE` | Max webhook notifications per minute. Unset/0 = off. |
| `WEBHOOK_RATE_LIMIT_PER_HOUR`   | Max webhook notifications per hour. Unset/0 = off.   |

Example:

```bash
# Allow up to 600/min and 10,000/hour to the webhook channel.
WEBHOOK_RATE_LIMIT_PER_MINUTE=600
WEBHOOK_RATE_LIMIT_PER_HOUR=10000
```

When neither limit is set, rate limiting is disabled and notifications are
delivered without throttling.

## Metrics

A counter is exported on `/metrics`:

- `soroban_pulse_notification_rate_limited_total` — incremented once for each
  notification that had to wait for the rate-limit window (i.e. was batched).

A steadily rising value indicates the channel is regularly over budget and the
limit may need raising, or the upstream event volume reduced with content
filtering (see [content filtering](./notification-content-filter.md)).

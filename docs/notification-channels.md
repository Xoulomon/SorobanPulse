# Notification Channels – Advanced Features

This document covers four advanced features for notification channels:
**failover** (#499), **analytics dashboard** (#500), **cost tracking** (#501), and **testing mode** (#502).

---

## #499 – Channel Failover

When the primary notification channel fails (after all retries are exhausted), the
service can automatically retry delivery via a configured failover channel.

### Configuration

Set the `failover_channel_id` column on a `notification_channels` row to the UUID of
the secondary channel:

```sql
UPDATE notification_channels
SET failover_channel_id = '<secondary-channel-uuid>'
WHERE id = '<primary-channel-uuid>';
```

### How it works

1. The primary channel is attempted according to its `retry_policy`.
2. If all retries fail, `deliver_with_failover` is called with the failover URL.
3. The failover channel is attempted once (using its own retry policy).
4. On failover, the `soroban_pulse_notification_failover_total` counter increments
   (labeled by `channel_type`).
5. If both channels fail the event is written to the DLQ (`webhook_failures`) and
   `soroban_pulse_webhook_failures_total` increments.

### Metric

```
soroban_pulse_notification_failover_total{channel_type="webhook"}
```

---

## #500 – Notification Analytics Dashboard

`GET /v1/admin/notifications/dashboard`

Requires `ADMIN_API_KEY`.

Returns a combined analytics summary in a single request:

```json
{
  "totals": {
    "last_24h": 120,
    "last_7d":  840,
    "last_30d": 3600
  },
  "channels": [
    {
      "channel_id":   "uuid",
      "channel_name": "primary-webhook",
      "channel_type": "webhook",
      "sent_30d":     3500,
      "failed_30d":   12,
      "success_rate": 0.997
    }
  ],
  "top_contracts": [
    { "contract_id": "CABC...", "event_count": 500 }
  ]
}
```

### Fields

| Field | Description |
|---|---|
| `totals.last_24h` | Notifications sent in the last 24 hours |
| `totals.last_7d` | Notifications sent in the last 7 days |
| `totals.last_30d` | Notifications sent in the last 30 days |
| `channels[].sent_30d` | Notifications sent by this channel in 30 days |
| `channels[].failed_30d` | Deliveries that failed (webhook_failures) in 30 days |
| `channels[].success_rate` | `(sent - failed) / sent`, rounded to 3 decimal places |
| `top_contracts` | Top 10 contracts by event count in the last 30 days |

---

## #501 – Notification Cost Tracking

### Configuration

Set a per-channel cost in **USD cents** via the migration-added column:

```sql
UPDATE notification_channels
SET cost_per_notification_cents = 1   -- $0.01 per notification
WHERE channel_type = 'sms';
```

Set a monthly budget alert threshold via environment variable:

```
NOTIFICATION_MONTHLY_BUDGET_USD=50.00
```

When cumulative costs for the current period reach ≥ 90 % of the budget, a `WARN`
log entry is emitted.

### Cost recording

After each successful notification delivery, insert a row into `notification_costs`:

```sql
INSERT INTO notification_costs (channel_id, channel_name, channel_type, cost_cents)
VALUES ($1, $2, $3, $4);
```

The `soroban_pulse_notification_cost_usd_total` counter is also incremented.

### Metric

```
soroban_pulse_notification_cost_usd_total
```

### GET /v1/admin/notifications/costs

Requires `ADMIN_API_KEY`.

Optional query parameter: `period` — `24h`, `7d`, `30d` (default `30d`).

```
GET /v1/admin/notifications/costs?period=7d
```

Response:

```json
{
  "period": "7d",
  "total_cost_usd": 3.45,
  "breakdown": [
    {
      "channel_id":         "uuid",
      "channel_name":       "twilio-sms",
      "channel_type":       "sms",
      "notification_count": 345,
      "total_cost_usd":     3.45
    }
  ]
}
```

---

## #502 – Channel Testing Mode

`POST /v1/admin/notifications/channels/:id/test`

Requires `ADMIN_API_KEY`.

Sends a **synchronous** test notification to the specified channel and returns the
delivery result immediately.

```bash
curl -X POST \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  http://localhost:3000/v1/admin/notifications/channels/<uuid>/test
```

### Response

**200 OK** (success):
```json
{
  "channel_id":   "uuid",
  "channel_name": "primary-webhook",
  "channel_type": "webhook",
  "success":      true,
  "subject":      "[TEST] Soroban Pulse notification test – channel 'primary-webhook'"
}
```

**502 Bad Gateway** (delivery failed):
```json
{
  "channel_id":   "uuid",
  "channel_name": "primary-webhook",
  "channel_type": "webhook",
  "success":      false,
  "subject":      "[TEST] Soroban Pulse notification test – channel 'primary-webhook'"
}
```

### Test notification content

All test notifications include a `[TEST]` prefix in the subject and body so they are
clearly distinguishable from real event notifications.

### Metric

```
soroban_pulse_notification_test_total{channel_type="webhook", result="success"}
soroban_pulse_notification_test_total{channel_type="webhook", result="failure"}
```

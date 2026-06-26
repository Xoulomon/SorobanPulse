# Notification Deduplication

Soroban Pulse delivers a notification for each event as it is indexed. If the
indexer re-processes a ledger range — for example after a restart with a stale
checkpoint — the same events can be indexed again. The `ON CONFLICT DO NOTHING`
constraint prevents duplicate rows in the `events` table, but without extra
bookkeeping the notification system would still deliver a second notification
for an event it already notified on.

Duplicate notifications cause confusion and may trigger duplicate actions in
downstream systems (e.g. duplicate trades or duplicate alerts). To prevent this,
Soroban Pulse tracks when a notification was sent for each event.

## How it works

* The `events` table has a `notified_at TIMESTAMPTZ` column (migration
  `20260625000000_add_events_notified_at`).
* **Before** delivering a notification, the delivery path checks whether
  `notified_at` is already set for the matching event
  (`tx_hash` + `contract_id` + `event_type`). If it is, the notification is
  skipped.
* **After** a successful delivery, `notified_at` is set to `NOW()`. The update
  only touches rows where `notified_at` is still NULL, so it is idempotent.

If the deduplication check fails because of a transient database error, the
system *fails open* and delivers the notification — it never silently drops a
notification because of an infrastructure hiccup.

## Metrics

A counter tracks how many notifications were skipped as duplicates:

```
soroban_pulse_notification_deduplicated_total
```

The counter increments once for every notification that is skipped because the
event was already notified. It is exposed on the `/metrics` endpoint.

## Re-indexing behaviour

Re-indexing a ledger range does **not** send duplicate notifications for events
that were already notified: each such event increments
`soroban_pulse_notification_deduplicated_total` and is skipped. Events that were
indexed but never successfully notified (their `notified_at` is still NULL) are
delivered normally.

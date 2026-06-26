# Content-Based Notification Filtering

_Issue #477_

By default the notification system delivers every event matching the configured
`contract_id` filter. High-frequency contracts can emit thousands of events per
minute, so operators who only care about a subset (for example, high-value
transfers) are overwhelmed with low-value noise — alert fatigue.

A **content filter** evaluates a predicate against each event's JSON data and
delivers the notification only when the predicate is satisfied.

## Filter syntax

A content filter is a JSON object with three fields:

```json
{ "path": "$.amount", "op": "gt", "value": "1000000" }
```

| Field   | Description                                                          |
| ------- | ------------------------------------------------------------------- |
| `path`  | JSONPath-style selector into the event data. Must start with `$`.   |
| `op`    | Comparison operator (see below).                                    |
| `value` | Right-hand value to compare against (always a string).              |

### Paths

Paths select a field within the event's `value` data:

- `$.amount` — top-level field `amount`
- `$.transfer.to` — nested field
- `$.amounts[0]` — array element by index
- `$['amount']` — bracketed key

### Operators

| Operator   | Meaning                                                              |
| ---------- | ------------------------------------------------------------------- |
| `eq`       | Equal. Numeric when both sides parse as numbers, otherwise string.  |
| `ne`       | Not equal. Also true when the field is absent.                      |
| `gt`       | Greater than.                                                       |
| `lt`       | Less than.                                                          |
| `gte`      | Greater than or equal.                                              |
| `lte`      | Less than or equal.                                                 |
| `contains` | String contains substring, or array contains the value.            |
| `matches`  | Field matches `value` interpreted as a regular expression.         |

Numeric operators compare numerically when both operands are numbers (JSON
numbers or numeric strings); otherwise they fall back to lexicographic order.
A path that does not resolve evaluates to "no match" for every operator except
`ne`.

## Configuring a filter

### Per webhook channel (environment)

```bash
WEBHOOK_CONTENT_FILTER='{"path":"$.amount","op":"gt","value":"1000000"}'
```

Only events whose `amount` exceeds 1,000,000 are delivered to the webhook. An
unparseable or invalid filter is logged at startup and ignored.

### Via the channel API

```
POST /v1/admin/notifications/channels
```

```json
{
  "name": "high-value-webhook",
  "channel_type": "webhook",
  "config": { "url": "https://example.com/hook" },
  "content_filter": { "path": "$.amount", "op": "gt", "value": "1000000" }
}
```

Invalid filter expressions (bad path syntax, or an uncompilable regex for
`matches`) are rejected with **`400 Bad Request`** at channel creation time.

## Examples

| Goal                                   | Filter                                                     |
| -------------------------------------- | ---------------------------------------------------------- |
| High-value transfers only              | `{"path":"$.amount","op":"gt","value":"1000000"}`          |
| A specific destination account         | `{"path":"$.transfer.to","op":"eq","value":"GDEST..."}`    |
| Events tagged `defi`                   | `{"path":"$.tags","op":"contains","value":"defi"}`         |
| Account IDs matching a pattern         | `{"path":"$.account","op":"matches","value":"^G[A-Z0-9]+$"}` |

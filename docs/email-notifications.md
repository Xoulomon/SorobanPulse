# Email Notifications

The email notification feature allows operators to receive email alerts when specific events are indexed by Soroban Pulse. This is useful for monitoring critical contracts or receiving alerts for important blockchain events.

## Features

- **Batched Notifications**: Events are batched and sent as a single email every minute to avoid flooding recipients
- **Contract Filtering**: Optional filtering to only receive notifications for specific contracts
- **SMTP Support**: Works with any SMTP server (Gmail, SendGrid, AWS SES, etc.)
- **Multiple Recipients**: Send notifications to multiple email addresses
- **Unsubscribe Links**: Every email carries a per-recipient unsubscribe link and a `List-Unsubscribe` header (CAN-SPAM / GDPR). Opted-out recipients are skipped on subsequent sends.
- **Secure**: SMTP credentials are never logged or exposed in metrics

## Configuration

Email notifications are configured via environment variables:

### Required Variables

- `EMAIL_SMTP_HOST`: SMTP server hostname (e.g., `smtp.gmail.com`, `smtp.sendgrid.net`)
- `EMAIL_FROM`: Sender email address (e.g., `soroban-pulse@example.com`)
- `EMAIL_TO`: Comma-separated list of recipient email addresses

### Optional Variables

- `EMAIL_SMTP_PORT`: SMTP server port (default: `587` for STARTTLS)
- `EMAIL_SMTP_USER`: SMTP authentication username (required by most servers)
- `EMAIL_SMTP_PASSWORD`: SMTP authentication password (required by most servers)
- `EMAIL_CONTRACT_FILTER`: Comma-separated list of contract IDs to filter notifications
- `EMAIL_PUBLIC_BASE_URL`: Public base URL used to build unsubscribe links (e.g. `https://pulse.example.com`). Defaults to `http://localhost:<PORT>` when unset.

## Unsubscribing

Each notification email includes an unsubscribe link of the form
`<EMAIL_PUBLIC_BASE_URL>/unsubscribe?token=<token>` and a matching
`List-Unsubscribe` header. The `/unsubscribe` endpoint is public (no API key
required). Visiting it records the recipient's opt-out, and no further emails
are sent to that address. The action is idempotent — re-visiting the link is
safe. Set `EMAIL_PUBLIC_BASE_URL` to a publicly reachable URL so recipients can
actually open the link.

## Example Configuration

### Gmail

```bash
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=your-email@gmail.com
EMAIL_SMTP_PASSWORD=your-app-password
EMAIL_FROM=soroban-pulse@gmail.com
EMAIL_TO=admin@example.com,alerts@example.com
```

**Note**: For Gmail, you need to use an [App Password](https://support.google.com/accounts/answer/185833) instead of your regular password.

### SendGrid

```bash
EMAIL_SMTP_HOST=smtp.sendgrid.net
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=apikey
EMAIL_SMTP_PASSWORD=your-sendgrid-api-key
EMAIL_FROM=noreply@yourdomain.com
EMAIL_TO=admin@example.com
```

### AWS SES

```bash
EMAIL_SMTP_HOST=email-smtp.us-east-1.amazonaws.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=your-ses-smtp-username
EMAIL_SMTP_PASSWORD=your-ses-smtp-password
EMAIL_FROM=verified-sender@yourdomain.com
EMAIL_TO=admin@example.com
```

## Contract Filtering

To receive notifications only for specific contracts, set the `EMAIL_CONTRACT_FILTER` variable:

```bash
EMAIL_CONTRACT_FILTER=CABC123...,CDEF456...
```

When this variable is set, only events from the specified contracts will trigger email notifications. When unset or empty, all indexed events will trigger notifications.

## Email Format

Each email contains:

- **Subject**: Number of new events indexed
- **Body**: Summary of events grouped by contract ID, including:
  - Contract ID
  - Number of events for that contract
  - Event details (type, ledger, transaction hash) for up to 10 events per contract
  - Indication if more events exist beyond the first 10

### Example Email

```
Subject: Soroban Pulse: 15 new events indexed

Soroban Pulse indexed 15 new events in the last minute.

Contract: CABC123...
  Events: 10
  - Type: contract, Ledger: 1234567, TxHash: abc123...
  - Type: contract, Ledger: 1234568, TxHash: def456...
  ... and 8 more events

Contract: CDEF456...
  Events: 5
  - Type: diagnostic, Ledger: 1234569, TxHash: ghi789...
  - Type: contract, Ledger: 1234570, TxHash: jkl012...
  - Type: contract, Ledger: 1234571, TxHash: mno345...
  - Type: contract, Ledger: 1234572, TxHash: pqr678...
  - Type: system, Ledger: 1234573, TxHash: stu901...
```

## Batching Behavior

- Events are collected for up to 60 seconds
- After 60 seconds, a single email is sent with all collected events
- If no events are collected in a 60-second window, no email is sent
- This prevents email flooding while ensuring timely notifications

## Monitoring

The email notification system exposes the following Prometheus metric:

- `soroban_pulse_email_failures_total`: Counter of failed email deliveries

Monitor this metric to detect SMTP configuration issues or delivery failures.

## Troubleshooting

### No emails are being sent

1. Verify `EMAIL_SMTP_HOST` is set and the service has restarted
2. Check that `EMAIL_FROM` and `EMAIL_TO` are configured
3. Verify SMTP credentials are correct
4. Check logs for error messages
5. Ensure your SMTP server allows connections from your deployment

### Authentication failures

1. Verify `EMAIL_SMTP_USER` and `EMAIL_SMTP_PASSWORD` are correct
2. For Gmail, ensure you're using an App Password, not your regular password
3. Check if your SMTP provider requires additional authentication setup

### Emails are delayed

- This is expected behavior. Emails are batched and sent once per minute to avoid flooding recipients
- If you need real-time notifications, consider using webhooks instead

### Only receiving emails for some contracts

- Check the `EMAIL_CONTRACT_FILTER` configuration
- Ensure the contract IDs in the filter match exactly (case-sensitive)
- Remove or clear `EMAIL_CONTRACT_FILTER` to receive notifications for all contracts

## Security Considerations

- SMTP credentials (`EMAIL_SMTP_PASSWORD`) are never logged or exposed in metrics
- Use environment variables or secret management systems to store credentials
- Consider using application-specific passwords or API keys instead of account passwords
- Restrict SMTP access to trusted IP addresses when possible
- Use TLS/STARTTLS for encrypted SMTP connections (port 587)

## Comparison with Webhooks

| Feature | Email Notifications | Webhooks |
|---------|-------------------|----------|
| Delivery | Batched (1/minute) | Real-time |
| Setup | SMTP configuration | HTTP endpoint |
| Filtering | Contract ID | Contract ID |
| Retries | SMTP-level | 3 attempts with backoff |
| Best for | Alerts, monitoring | Integrations, automation |

Use email notifications for human-readable alerts and monitoring. Use webhooks for system integrations and real-time event processing.

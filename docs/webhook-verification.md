# Webhook Signature Verification

This guide explains how to verify webhook signatures from Soroban Pulse to ensure the authenticity and integrity of webhook payloads.

## Overview

Soroban Pulse signs all webhook payloads using HMAC-SHA256. The signature is included in the `X-Signature-256` header of each webhook request. Verifying this signature is essential to:

- Prevent replay attacks
- Detect spoofed webhook deliveries
- Ensure data integrity

## Signature Format

The `X-Signature-256` header contains a hex-encoded HMAC-SHA256 signature of the request body, computed using your webhook secret key.

```
X-Signature-256: sha256=<hex-encoded-signature>
```

## Verification Process

1. Extract the signature from the `X-Signature-256` header
2. Compute HMAC-SHA256 of the raw request body using your webhook secret
3. Compare the computed signature with the header signature using constant-time comparison
4. Reject the webhook if signatures don't match

## Implementation Examples

### Python

```python
import hmac
import hashlib
import json
from typing import Tuple

def verify_webhook_signature(
    body: bytes,
    signature_header: str,
    webhook_secret: str
) -> Tuple[bool, str]:
    """
    Verify a webhook signature from Soroban Pulse.
    
    Args:
        body: Raw request body (bytes)
        signature_header: Value of X-Signature-256 header
        webhook_secret: Your webhook secret key
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Extract the signature from the header
    if not signature_header.startswith("sha256="):
        return False, "Invalid signature format"
    
    provided_signature = signature_header[7:]  # Remove "sha256=" prefix
    
    # Compute the expected signature
    expected_signature = hmac.new(
        webhook_secret.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    
    # Use constant-time comparison to prevent timing attacks
    is_valid = hmac.compare_digest(provided_signature, expected_signature)
    
    if not is_valid:
        return False, "Signature verification failed"
    
    return True, ""


# Example Flask endpoint
from flask import Flask, request, jsonify

app = Flask(__name__)
WEBHOOK_SECRET = "your-webhook-secret-here"

@app.route("/webhooks/soroban-pulse", methods=["POST"])
def handle_webhook():
    body = request.get_data()
    signature_header = request.headers.get("X-Signature-256", "")
    
    is_valid, error = verify_webhook_signature(body, signature_header, WEBHOOK_SECRET)
    
    if not is_valid:
        return jsonify({"error": error}), 401
    
    # Process the webhook
    payload = request.get_json()
    print(f"Received event: {payload}")
    
    return jsonify({"status": "ok"}), 200
```

### JavaScript/TypeScript

```typescript
import crypto from "crypto";

interface VerificationResult {
  isValid: boolean;
  error?: string;
}

function verifyWebhookSignature(
  body: Buffer | string,
  signatureHeader: string,
  webhookSecret: string
): VerificationResult {
  /**
   * Verify a webhook signature from Soroban Pulse.
   *
   * @param body - Raw request body
   * @param signatureHeader - Value of X-Signature-256 header
   * @param webhookSecret - Your webhook secret key
   * @returns Verification result
   */

  // Ensure body is a Buffer
  const bodyBuffer = typeof body === "string" ? Buffer.from(body) : body;

  // Extract the signature from the header
  if (!signatureHeader.startsWith("sha256=")) {
    return { isValid: false, error: "Invalid signature format" };
  }

  const providedSignature = signatureHeader.slice(7); // Remove "sha256=" prefix

  // Compute the expected signature
  const expectedSignature = crypto
    .createHmac("sha256", webhookSecret)
    .update(bodyBuffer)
    .digest("hex");

  // Use constant-time comparison to prevent timing attacks
  const isValid = crypto.timingSafeEqual(
    Buffer.from(providedSignature),
    Buffer.from(expectedSignature)
  );

  if (!isValid) {
    return { isValid: false, error: "Signature verification failed" };
  }

  return { isValid: true };
}

// Example Express endpoint
import express from "express";

const app = express();
const WEBHOOK_SECRET = "your-webhook-secret-here";

app.post("/webhooks/soroban-pulse", express.raw({ type: "*/*" }), (req, res) => {
  const signatureHeader = req.headers["x-signature-256"] as string;
  const result = verifyWebhookSignature(req.body, signatureHeader, WEBHOOK_SECRET);

  if (!result.isValid) {
    return res.status(401).json({ error: result.error });
  }

  // Process the webhook
  const payload = JSON.parse(req.body.toString());
  console.log("Received event:", payload);

  res.json({ status: "ok" });
});
```

### Go

```go
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// VerifyWebhookSignature verifies a webhook signature from Soroban Pulse.
func VerifyWebhookSignature(body []byte, signatureHeader, webhookSecret string) (bool, error) {
	// Extract the signature from the header
	if !strings.HasPrefix(signatureHeader, "sha256=") {
		return false, fmt.Errorf("invalid signature format")
	}

	providedSignature := signatureHeader[7:] // Remove "sha256=" prefix

	// Compute the expected signature
	h := hmac.New(sha256.New, []byte(webhookSecret))
	h.Write(body)
	expectedSignature := hex.EncodeToString(h.Sum(nil))

	// Use constant-time comparison to prevent timing attacks
	if !hmac.Equal([]byte(providedSignature), []byte(expectedSignature)) {
		return false, fmt.Errorf("signature verification failed")
	}

	return true, nil
}

// Example HTTP handler
func handleWebhook(w http.ResponseWriter, r *http.Request) {
	webhookSecret := "your-webhook-secret-here"

	// Read the raw body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Verify the signature
	signatureHeader := r.Header.Get("X-Signature-256")
	isValid, err := VerifyWebhookSignature(body, signatureHeader, webhookSecret)

	if !isValid {
		http.Error(w, fmt.Sprintf("Signature verification failed: %v", err), http.StatusUnauthorized)
		return
	}

	// Process the webhook
	fmt.Fprintf(w, `{"status":"ok"}`)
}

func main() {
	http.HandleFunc("/webhooks/soroban-pulse", handleWebhook)
	http.ListenAndServe(":8080", nil)
}
```

## Best Practices

### 1. Always Verify Signatures in Production

Never skip signature verification in production environments. This is your primary defense against spoofed webhooks.

```python
# ✅ GOOD: Always verify
if not verify_webhook_signature(body, signature_header, secret):
    return 401

# ❌ BAD: Skipping verification
# process_webhook(payload)
```

### 2. Use Constant-Time Comparison

Always use constant-time comparison functions (e.g., `hmac.compare_digest` in Python, `crypto.timingSafeEqual` in Node.js) to prevent timing attacks.

```python
# ✅ GOOD: Constant-time comparison
is_valid = hmac.compare_digest(provided, expected)

# ❌ BAD: Regular string comparison (vulnerable to timing attacks)
# is_valid = provided == expected
```

### 3. Store Secrets Securely

Never hardcode webhook secrets in your source code. Use environment variables or a secrets management system.

```python
# ✅ GOOD: Load from environment
webhook_secret = os.environ.get("WEBHOOK_SECRET")

# ❌ BAD: Hardcoded secret
# webhook_secret = "my-secret-key"
```

### 4. Validate Request Headers

Check that the `X-Signature-256` header is present and properly formatted before attempting verification.

```python
signature_header = request.headers.get("X-Signature-256")
if not signature_header:
    return 400, "Missing X-Signature-256 header"
```

### 5. Log Verification Failures

Log failed verification attempts for security monitoring and debugging.

```python
if not is_valid:
    logger.warning(f"Webhook signature verification failed from {request.remote_addr}")
    return 401
```

## HTTPS Requirement

Soroban Pulse can be configured to require HTTPS for all webhook URLs via the `WEBHOOK_REQUIRE_HTTPS` environment variable. When enabled, non-HTTPS webhook URLs will be rejected during webhook registration.

```bash
# Require HTTPS for all webhooks (recommended for production)
export WEBHOOK_REQUIRE_HTTPS=true
```

## Testing Webhook Verification

### Using curl

```bash
# Generate a test signature
SECRET="your-webhook-secret"
BODY='{"event":"test"}'
SIGNATURE=$(echo -n "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex | cut -d' ' -f2)

# Send a test webhook
curl -X POST http://localhost:8080/webhooks/soroban-pulse \
  -H "Content-Type: application/json" \
  -H "X-Signature-256: sha256=$SIGNATURE" \
  -d "$BODY"
```

### Using Python

```python
import requests
import hmac
import hashlib

webhook_url = "http://localhost:8080/webhooks/soroban-pulse"
webhook_secret = "your-webhook-secret"
payload = {"event": "test"}

# Compute signature
body = json.dumps(payload).encode()
signature = hmac.new(webhook_secret.encode(), body, hashlib.sha256).hexdigest()

# Send request
response = requests.post(
    webhook_url,
    json=payload,
    headers={"X-Signature-256": f"sha256={signature}"}
)

print(response.status_code, response.json())
```

## Troubleshooting

### Signature Verification Always Fails

1. **Check the webhook secret**: Ensure you're using the correct secret key
2. **Verify the body**: Make sure you're using the raw request body, not a parsed/modified version
3. **Check header format**: Ensure the header starts with `sha256=`
4. **Encoding issues**: Verify that the signature is hex-encoded

### Timing Attacks

Always use constant-time comparison functions provided by your language's standard library. Never use regular string comparison (`==`) for security-sensitive comparisons.

## Additional Resources

- [OWASP: Webhook Security](https://owasp.org/www-community/attacks/Webhook_Attacks)
- [RFC 2104: HMAC](https://tools.ietf.org/html/rfc2104)
- [NIST: Cryptographic Hash Functions](https://csrc.nist.gov/projects/hash-functions)

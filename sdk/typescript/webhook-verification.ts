/**
 * Webhook signature verification utilities for Soroban Pulse webhooks.
 *
 * This module provides helpers to verify HMAC-SHA256 signatures on webhook payloads
 * from Soroban Pulse, ensuring authenticity and integrity.
 */

import crypto from "crypto";

export interface VerificationResult {
  isValid: boolean;
  error?: string;
}

/**
 * Verify a webhook signature from Soroban Pulse.
 *
 * @param body - Raw request body (Buffer or string)
 * @param signatureHeader - Value of X-Signature-256 header
 * @param webhookSecret - Your webhook secret key
 * @returns Verification result with isValid flag and optional error message
 *
 * @throws Error if signature header format is invalid
 *
 * @example
 * ```typescript
 * const body = Buffer.from('{"event":"test"}');
 * const signature = "sha256=abc123...";
 * const secret = "my-secret";
 * const result = verifyWebhookSignature(body, signature, secret);
 * if (result.isValid) {
 *   console.log("Signature verified!");
 * } else {
 *   console.log(`Verification failed: ${result.error}`);
 * }
 * ```
 */
export function verifyWebhookSignature(
  body: Buffer | string,
  signatureHeader: string,
  webhookSecret: string
): VerificationResult {
  // Ensure body is a Buffer
  const bodyBuffer = typeof body === "string" ? Buffer.from(body) : body;

  // Extract the signature from the header
  if (!signatureHeader.startsWith("sha256=")) {
    throw new Error("Invalid signature format: must start with 'sha256='");
  }

  const providedSignature = signatureHeader.slice(7); // Remove "sha256=" prefix

  // Compute the expected signature
  const expectedSignature = crypto
    .createHmac("sha256", webhookSecret)
    .update(bodyBuffer)
    .digest("hex");

  // Use constant-time comparison to prevent timing attacks
  let isValid = false;
  try {
    isValid = crypto.timingSafeEqual(
      Buffer.from(providedSignature),
      Buffer.from(expectedSignature)
    );
  } catch {
    // timingSafeEqual throws if buffers are different lengths
    isValid = false;
  }

  if (!isValid) {
    return { isValid: false, error: "Signature verification failed" };
  }

  return { isValid: true };
}

/**
 * Verify a webhook signature from Soroban Pulse (safe version).
 *
 * This version returns a simple boolean and handles errors gracefully.
 * Use this if you prefer not to handle exceptions.
 *
 * @param body - Raw request body (Buffer or string)
 * @param signatureHeader - Value of X-Signature-256 header
 * @param webhookSecret - Your webhook secret key
 * @returns True if signature is valid, False otherwise
 *
 * @example
 * ```typescript
 * const body = Buffer.from('{"event":"test"}');
 * const signature = "sha256=abc123...";
 * const secret = "my-secret";
 * if (verifyWebhookSignatureSafe(body, signature, secret)) {
 *   console.log("Signature verified!");
 * } else {
 *   console.log("Signature verification failed");
 * }
 * ```
 */
export function verifyWebhookSignatureSafe(
  body: Buffer | string,
  signatureHeader: string,
  webhookSecret: string
): boolean {
  try {
    const result = verifyWebhookSignature(body, signatureHeader, webhookSecret);
    return result.isValid;
  } catch {
    return false;
  }
}

/**
 * Create a test webhook signature for testing purposes.
 *
 * @param body - Request body (Buffer or string)
 * @param webhookSecret - Your webhook secret key
 * @returns Signature header value (e.g., "sha256=abc123...")
 *
 * @example
 * ```typescript
 * const body = JSON.stringify({ event: "test" });
 * const secret = "my-secret";
 * const signature = createTestSignature(body, secret);
 * // Use signature in X-Signature-256 header
 * ```
 */
export function createTestSignature(
  body: Buffer | string,
  webhookSecret: string
): string {
  const bodyBuffer = typeof body === "string" ? Buffer.from(body) : body;
  const signature = crypto
    .createHmac("sha256", webhookSecret)
    .update(bodyBuffer)
    .digest("hex");
  return `sha256=${signature}`;
}

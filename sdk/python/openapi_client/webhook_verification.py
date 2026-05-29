"""
Webhook signature verification utilities for Soroban Pulse webhooks.

This module provides helpers to verify HMAC-SHA256 signatures on webhook payloads
from Soroban Pulse, ensuring authenticity and integrity.
"""

import hmac
import hashlib
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
        Tuple of (is_valid, error_message). If is_valid is True, error_message is empty.
    
    Raises:
        ValueError: If signature_header format is invalid
    
    Example:
        >>> body = b'{"event": "test"}'
        >>> signature = "sha256=abc123..."
        >>> secret = "my-secret"
        >>> is_valid, error = verify_webhook_signature(body, signature, secret)
        >>> if is_valid:
        ...     print("Signature verified!")
        ... else:
        ...     print(f"Verification failed: {error}")
    """
    # Extract the signature from the header
    if not signature_header.startswith("sha256="):
        raise ValueError("Invalid signature format: must start with 'sha256='")
    
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


def verify_webhook_signature_safe(
    body: bytes,
    signature_header: str,
    webhook_secret: str
) -> bool:
    """
    Verify a webhook signature from Soroban Pulse (safe version).
    
    This version returns a simple boolean and handles errors gracefully.
    Use this if you prefer not to handle exceptions.
    
    Args:
        body: Raw request body (bytes)
        signature_header: Value of X-Signature-256 header
        webhook_secret: Your webhook secret key
    
    Returns:
        True if signature is valid, False otherwise
    
    Example:
        >>> body = b'{"event": "test"}'
        >>> signature = "sha256=abc123..."
        >>> secret = "my-secret"
        >>> if verify_webhook_signature_safe(body, signature, secret):
        ...     print("Signature verified!")
        ... else:
        ...     print("Signature verification failed")
    """
    try:
        is_valid, _ = verify_webhook_signature(body, signature_header, webhook_secret)
        return is_valid
    except (ValueError, TypeError):
        return False

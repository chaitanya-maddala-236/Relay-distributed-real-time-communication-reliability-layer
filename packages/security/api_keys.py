"""API key generation and verification (Section 13, 92).

Plaintext keys are never stored. We store a SHA-256 hash plus a short
non-secret prefix (for lookup/log correlation without revealing the key).
"""
from __future__ import annotations

import hashlib
import secrets

KEY_PREFIX_LENGTH = 8


def generate_api_key() -> tuple[str, str, str]:
    """Returns (plaintext_key, prefix, hash). Plaintext is shown to the
    user exactly once and never persisted."""
    plaintext = f"relay_{secrets.token_urlsafe(32)}"
    prefix = plaintext[: KEY_PREFIX_LENGTH + 6]
    key_hash = hash_api_key(plaintext)
    return plaintext, prefix, key_hash


def hash_api_key(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def verify_api_key(plaintext: str, key_hash: str) -> bool:
    return secrets.compare_digest(hash_api_key(plaintext), key_hash)

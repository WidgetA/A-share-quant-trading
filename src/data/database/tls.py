"""Fail-closed PostgreSQL TLS helpers for the isolated V20 runtime."""

from __future__ import annotations

import hashlib
import ssl
from pathlib import Path


def sha256_file(path: str | Path) -> str:
    """Hash a regular file without exposing its contents."""

    resolved = Path(path).expanduser()
    if not resolved.is_file():
        raise ValueError(f"PostgreSQL CA file is not readable: {resolved}")
    digest = hashlib.sha256()
    try:
        with resolved.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise ValueError(f"PostgreSQL CA file is not readable: {resolved}") from exc
    return digest.hexdigest()


def verified_postgres_ssl_context(
    *,
    ssl_mode: str,
    ssl_root_cert: str | Path,
    expected_sha256: str,
) -> ssl.SSLContext:
    """Build a hostname-verifying context bound to a reviewed CA bundle."""

    if ssl_mode != "verify-full":
        raise ValueError("PostgreSQL SSL mode must be verify-full")
    if len(expected_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in expected_sha256
    ):
        raise ValueError("PostgreSQL CA SHA-256 must be a lowercase digest")
    resolved = Path(ssl_root_cert).expanduser()
    actual_sha256 = sha256_file(resolved)
    if actual_sha256 != expected_sha256:
        raise ValueError("PostgreSQL CA file SHA-256 does not match reviewed configuration")
    try:
        context = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH,
            cafile=str(resolved),
        )
    except (OSError, ssl.SSLError) as exc:
        raise ValueError("PostgreSQL CA file is not a valid trust bundle") from exc
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    return context


__all__ = ["sha256_file", "verified_postgres_ssl_context"]

"""Outbound webhook delivery."""
from __future__ import annotations

import hashlib
import hmac
import json

import httpx

from . import config


def sign(body: bytes, secret: str | None = None) -> str:
    """Return the X-Webhook-Signature value: ``sha256=<hex>``."""
    digest = hmac.new(
        (secret or config.WEBHOOK_SECRET).encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    return f"sha256={digest}"


def serialize_event(event: dict) -> bytes:
    """The exact bytes POSTed to the target endpoint (and signed)."""
    body = {
        "id": event["id"],
        "idempotency_key": event["idempotency_key"],
        "target": event["target"],
        "payload": event["payload"],
        "created_at": event["created_at"],
        "attempt": event["attempts_count"] + 1,
    }
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def deliver(event: dict) -> tuple[int | None, str | None]:
    """POST one event to its target.

    Returns ``(status_code, error)``. Any 2xx is success; network errors or
    timeouts return ``(None, error_text)`` and are retried like any failure.
    """
    body = serialize_event(event)
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "reliable-webhook/1.0",
        config.SIGNATURE_HEADER: sign(body),
        "X-Event-Id": event["id"],
        "X-Idempotency-Key": event["idempotency_key"],
        "X-Delivery-Attempt": str(event["attempts_count"] + 1),
    }
    try:
        response = httpx.post(
            event["target"],
            content=body,
            headers=headers,
            timeout=config.DELIVERY_TIMEOUT,
        )
        return response.status_code, None
    except httpx.HTTPError as exc:
        return None, f"{type(exc).__name__}: {exc}"

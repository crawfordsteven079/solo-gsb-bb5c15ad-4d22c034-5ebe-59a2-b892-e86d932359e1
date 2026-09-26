"""End-to-end tests for the reliable webhook delivery API."""
from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from .conftest import receipts, wait_for

SECRET = "test-secret"


def _sign(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


def _post_event(base_url: str, target_path: str, key: str | None = None,
                payload=None, extra_query: str = "") -> httpx.Response:
    key = key or uuid.uuid4().hex
    body = (
        b'{"idempotency_key":"' + key.encode()
        + b'","target":"' + f"{base_url}{target_path}{extra_query}".encode()
        + b'","payload":' + (payload or b'{"v":1}') + b"}"
    )
    return httpx.post(
        f"{base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": _sign(body), "Content-Type": "application/json"},
    )


def _get_event(base_url: str, event_id: str) -> dict:
    return httpx.get(f"{base_url}/events/{event_id}").json()


def _post_event_obj(base_url: str, obj: dict) -> httpx.Response:
    """POST /events with an arbitrary JSON body (for delivery-window fields)."""
    body = json.dumps(obj).encode()
    return httpx.post(
        f"{base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": _sign(body), "Content-Type": "application/json"},
    )


def _parse_iso(value: str):
    from datetime import datetime
    return datetime.fromisoformat(value)


# ---------------------------------------------------------------- signature

def test_missing_or_bad_signature_is_rejected(server):
    body = (
        b'{"idempotency_key":"bad-sig","target":"'
        + f"{server.base_url}/t/record".encode()
        + b'","payload":{}}'
    )
    assert httpx.post(f"{server.base_url}/events", content=body).status_code == 401
    resp = httpx.post(
        f"{server.base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": "sha256=deadbeef"},
    )
    assert resp.status_code == 401


def test_tampered_body_fails_signature(server):
    body = (
        b'{"idempotency_key":"tamper","target":"'
        + f"{server.base_url}/t/record".encode()
        + b'","payload":{"a":1}}'
    )
    resp = httpx.post(
        f"{server.base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": _sign(body + b" ")},
    )
    assert resp.status_code == 401


# ------------------------------------------------------------- happy path

def test_event_is_delivered_and_queryable(server):
    resp = _post_event(server.base_url, "/t/record")
    assert resp.status_code == 201
    event = resp.json()
    assert event["status"] in ("queued", "processing")

    final = wait_for(lambda: (lambda e: e if e["status"] == "delivered" else None)(
        _get_event(server.base_url, event["id"])
    ))
    assert final["attempts_count"] == 1
    attempt = final["attempts"][0]
    assert attempt["attempt_no"] == 1
    assert attempt["status_code"] == 200
    assert attempt["error"] is None
    assert attempt["attempted_at"]
    assert final["delivered_at"]

    # Outbound request was signed with the shared secret and carried headers.
    mine = [r for r in receipts if r["event_id"] == event["id"]]
    assert len(mine) == 1
    assert mine[0]["idempotency_key"] == event["idempotency_key"]
    assert mine[0]["signature"] == _sign(mine[0]["body"].encode())

    # List endpoint shows it too.
    listed = httpx.get(f"{server.base_url}/events", params={"status": "delivered"}).json()
    assert any(e["id"] == event["id"] for e in listed["events"])


# ------------------------------------------------------------ idempotency

def test_duplicate_idempotency_key_returns_original_and_is_not_reenqueued(server):
    key = f"dup-{uuid.uuid4().hex}"
    before = len([r for r in receipts if r["idempotency_key"] == key])

    r1 = _post_event(server.base_url, "/t/record", key=key)
    assert r1.status_code == 201
    wait_for(lambda: _get_event(server.base_url, r1.json()["id"])["status"] == "delivered")

    r2 = _post_event(server.base_url, "/t/record", key=key)
    assert r2.status_code == 200
    assert r2.headers.get("idempotent-replay") == "true"
    assert r2.json()["id"] == r1.json()["id"]

    time.sleep(0.5)
    delivered = [r for r in receipts if r["idempotency_key"] == key]
    assert len(delivered) == before + 1  # exactly one outbound delivery ever


# ------------------------------------------------- retries with 1/2/4 backoff

def test_retries_use_1_2_4_backoff_then_dead_letter(server):
    resp = _post_event(server.base_url, "/t/fail-always")
    event_id = resp.json()["id"]
    final = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=30,
    )
    assert final["attempts_count"] == 4
    assert [a["status_code"] for a in final["attempts"]] == [500, 500, 500, 500]
    assert final["delivered_at"] is None

    times = [_parse_iso(a["attempted_at"]) for a in final["attempts"]]
    gaps = [(times[i] - times[i - 1]).total_seconds() for i in range(1, 4)]
    # Gaps include request latency; allow slack but verify the 1/2/4 schedule.
    assert 0.9 <= gaps[0] <= 2.5, gaps
    assert 1.8 <= gaps[1] <= 3.5, gaps
    assert 3.6 <= gaps[2] <= 5.5, gaps

    # Dead-letter events are listed under status=dead.
    dead = httpx.get(f"{server.base_url}/events", params={"status": "dead"}).json()
    assert any(e["id"] == event_id for e in dead["events"])


def test_event_recovers_after_two_failures(server):
    resp = _post_event(
        server.base_url, "/dev/sink", extra_query="?fail_times=2&fail_status=503"
    )
    event_id = resp.json()["id"]
    final = wait_for(
        lambda: (lambda e: e if e["status"] == "delivered" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=20,
    )
    codes = [a["status_code"] for a in final["attempts"]]
    assert codes == [503, 503, 200]


# ---------------------------------------------------- per-endpoint ordering

def test_events_to_same_endpoint_are_strictly_ordered(server):
    from .conftest import _gates
    token = f"order-{uuid.uuid4().hex}"
    _gates[token] = threading.Event()

    first = _post_event(server.base_url, "/t/block", extra_query=f"?token={token}")
    first_id = first.json()["id"]
    # Ensure the first delivery has claimed and is blocking.
    wait_for(
        lambda: _get_event(server.base_url, first_id)["status"] == "processing",
        timeout=10,
    )

    second = _post_event(server.base_url, "/t/block", extra_query=f"?token={token}")
    second_id = second.json()["id"]
    time.sleep(0.5)
    # The second event must stay queued while the first is in flight.
    assert _get_event(server.base_url, second_id)["status"] == "queued"

    # Release the gate: both must resolve, and recorded delivery order
    # (verified through /events ids) is first -> second.
    httpx.post(f"{server.base_url}/t/release", params={"token": token})
    wait_for(lambda: _get_event(server.base_url, first_id)["status"] == "delivered")
    wait_for(lambda: _get_event(server.base_url, second_id)["status"] == "delivered")

    mine = [r for r in receipts]  # block sink does not record; assert via timestamps
    t1 = _parse_iso(_get_event(server.base_url, first_id)["delivered_at"])
    t2 = _parse_iso(_get_event(server.base_url, second_id)["delivered_at"])
    assert t1 <= t2


def test_earlier_failure_blocks_later_event_until_it_dies(server):
    """While event A retries/dies, B to the same target must not start."""
    target = f"{server.base_url}/t/fail-always"

    first = _post_event(server.base_url, "/t/fail-always")
    first_id = first.json()["id"]
    body = (
        b'{"idempotency_key":"' + uuid.uuid4().hex.encode()
        + b'","target":"' + target.encode() + b'","payload":{}}'
    )
    second_resp = httpx.post(
        f"{server.base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": _sign(body)},
    )
    second_id = second_resp.json()["id"]

    # While the first is failing/retrying, the second never starts:
    # it has zero attempts and stays queued.
    deadline = time.time() + 3
    while time.time() < deadline:
        first_e = _get_event(server.base_url, first_id)
        second_e = _get_event(server.base_url, second_id)
        if first_e["status"] in ("queued", "processing"):
            assert second_e["attempts_count"] == 0
        time.sleep(0.1)

    final_first = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, first_id)
        ), timeout=30
    )
    # Only after the first is dead does the second start (and it also fails).
    final_second = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, second_id)
        ), timeout=30
    )
    last_attempt_first = _parse_iso(final_first["attempts"][-1]["attempted_at"])
    first_attempt_second = _parse_iso(final_second["attempts"][0]["attempted_at"])
    assert last_attempt_first <= first_attempt_second


# --------------------------------------------------------- dead-letter replay

def test_replay_dead_event_requeues_and_delivers_with_full_history(server):
    # fail_times=4: the first round dies after 4 attempts; the replayed
    # round then succeeds on the (globally numbered) 5th attempt.
    resp = _post_event(server.base_url, "/t/flaky", extra_query="?fail_times=4")
    event_id = resp.json()["id"]
    dead = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=30,
    )
    assert dead["attempts_count"] == 4
    assert dead["replay_count"] == 0
    assert dead["last_replayed_at"] is None

    replay = httpx.post(f"{server.base_url}/events/{event_id}/replay")
    assert replay.status_code == 200
    replayed = replay.json()
    assert replayed["status"] == "queued"
    assert replayed["replay_count"] == 1
    assert replayed["last_replayed_at"]
    # Idempotency key, event id and attempt history are untouched.
    assert replayed["id"] == event_id
    assert replayed["idempotency_key"] == dead["idempotency_key"]
    assert [a["attempt_no"] for a in replayed["attempts"]] == [1, 2, 3, 4]

    final = wait_for(
        lambda: (lambda e: e if e["status"] == "delivered" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=20,
    )
    assert final["attempts_count"] == 5
    assert [a["attempt_no"] for a in final["attempts"]] == [1, 2, 3, 4, 5]
    assert [a["status_code"] for a in final["attempts"]] == [500, 500, 500, 500, 200]
    assert final["replay_count"] == 1
    assert final["delivered_at"]

    # The replayed delivery was signed and carried the continuous attempt
    # number plus the unchanged idempotency key.
    mine = [r for r in receipts if r["event_id"] == event_id]
    assert len(mine) == 5
    assert mine[-1]["attempt"] == "5"
    assert mine[-1]["idempotency_key"] == dead["idempotency_key"]
    assert mine[-1]["signature"] == _sign(mine[-1]["body"].encode())

    # List responses expose the replay metadata too.
    listed = httpx.get(
        f"{server.base_url}/events", params={"status": "delivered"}
    ).json()
    entry = [e for e in listed["events"] if e["id"] == event_id]
    assert entry and entry[0]["replay_count"] == 1
    assert entry[0]["last_replayed_at"]


def test_replay_starts_a_fresh_full_backoff_round(server):
    resp = _post_event(server.base_url, "/t/fail-always")
    event_id = resp.json()["id"]
    wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=30,
    )

    # Each replay grants a whole new round of 4 attempts (not an instant
    # re-death), and replay_count accumulates.
    for expected_replays in (1, 2):
        replay = httpx.post(f"{server.base_url}/events/{event_id}/replay")
        assert replay.status_code == 200
        dead_again = wait_for(
            lambda: (lambda e: e if e["status"] == "dead" else None)(
                _get_event(server.base_url, event_id)
            ),
            timeout=30,
        )
        assert dead_again["replay_count"] == expected_replays
        assert dead_again["attempts_count"] == 4 * (expected_replays + 1)

    final = _get_event(server.base_url, event_id)
    assert [a["attempt_no"] for a in final["attempts"]] == list(range(1, 13))
    # The last round also followed the 1/2/4 backoff schedule.
    times = [_parse_iso(a["attempted_at"]) for a in final["attempts"]]
    gaps = [(times[i] - times[i - 1]).total_seconds() for i in range(9, 12)]
    assert 0.9 <= gaps[0] <= 2.5, gaps
    assert 1.8 <= gaps[1] <= 3.5, gaps
    assert 3.6 <= gaps[2] <= 5.5, gaps


def test_replay_rejects_non_dead_and_unknown_events(server):
    from .conftest import _gates
    token = f"replay-{uuid.uuid4().hex}"
    _gates[token] = threading.Event()

    blocked = _post_event(server.base_url, "/t/block", extra_query=f"?token={token}")
    blocked_id = blocked.json()["id"]
    wait_for(
        lambda: _get_event(server.base_url, blocked_id)["status"] == "processing",
        timeout=10,
    )
    queued = _post_event(server.base_url, "/t/block", extra_query=f"?token={token}")
    queued_id = queued.json()["id"]
    time.sleep(0.3)
    assert _get_event(server.base_url, queued_id)["status"] == "queued"

    try:
        r = httpx.post(f"{server.base_url}/events/{blocked_id}/replay")
        assert r.status_code == 409
        assert "processing" in r.json()["detail"]

        r = httpx.post(f"{server.base_url}/events/{queued_id}/replay")
        assert r.status_code == 409
        assert "queued" in r.json()["detail"]
    finally:
        httpx.post(f"{server.base_url}/t/release", params={"token": token})

    wait_for(lambda: _get_event(server.base_url, blocked_id)["status"] == "delivered")
    r = httpx.post(f"{server.base_url}/events/{blocked_id}/replay")
    assert r.status_code == 409
    assert "delivered" in r.json()["detail"]

    r = httpx.post(f"{server.base_url}/events/{uuid.uuid4().hex}/replay")
    assert r.status_code == 404
    assert r.json()["detail"] == "event not found"


def test_concurrent_replays_apply_exactly_once(server):
    resp = _post_event(server.base_url, "/t/flaky", extra_query="?fail_times=4")
    event_id = resp.json()["id"]
    wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=30,
    )

    codes: list[int] = []

    def hit():
        codes.append(httpx.post(f"{server.base_url}/events/{event_id}/replay").status_code)

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly one replay wins; the rest are rejected and enqueue nothing.
    assert codes.count(200) == 1
    assert codes.count(409) == len(codes) - 1

    final = wait_for(
        lambda: (lambda e: e if e["status"] == "delivered" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=20,
    )
    assert final["replay_count"] == 1
    assert final["attempts_count"] == 5
    # No duplicate delivery resulted from the replay race.
    mine = [r for r in receipts if r["event_id"] == event_id]
    assert len(mine) == 5


# --------------------------------------------------------- delivery windows

def test_window_fields_default_and_roundtrip(server):
    resp = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": f"{server.base_url}/t/record",
        "payload": {},
    })
    assert resp.status_code == 201
    event = resp.json()
    # deliver_after defaults to the creation moment; expires_at is unset.
    assert _parse_iso(event["deliver_after"]) == _parse_iso(event["created_at"])
    assert event["expires_at"] is None

    # Every query endpoint exposes the window fields.
    by_id = _get_event(server.base_url, event["id"])
    assert by_id["deliver_after"] == event["deliver_after"]
    assert by_id["expires_at"] is None
    by_key = httpx.get(
        f"{server.base_url}/events/key/{event['idempotency_key']}"
    ).json()
    assert by_key["deliver_after"] == event["deliver_after"]
    listed = httpx.get(f"{server.base_url}/events").json()
    entry = [e for e in listed["events"] if e["id"] == event["id"]]
    assert entry and entry[0]["deliver_after"] == event["deliver_after"]


def test_deliver_after_postpones_delivery(server):
    deliver_after = datetime.now(timezone.utc) + timedelta(seconds=1.5)
    resp = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": f"{server.base_url}/t/record",
        "payload": {},
        "deliver_after": deliver_after.isoformat(),
    })
    assert resp.status_code == 201
    event = resp.json()
    assert _parse_iso(event["deliver_after"]) == deliver_after
    assert event["expires_at"] is None

    # Before the window opens: still queued, zero attempts.
    time.sleep(1.0)
    pending = _get_event(server.base_url, event["id"])
    assert pending["status"] == "queued"
    assert pending["attempts_count"] == 0

    # Afterwards it is delivered, never earlier than deliver_after.
    final = wait_for(
        lambda: (lambda e: e if e["status"] == "delivered" else None)(
            _get_event(server.base_url, event["id"])
        ),
        timeout=10,
    )
    assert _parse_iso(final["delivered_at"]) >= deliver_after
    assert final["attempts_count"] == 1


def test_delayed_head_event_still_blocks_later_events(server):
    """An earlier event waiting for deliver_after keeps blocking its target."""
    target = f"{server.base_url}/t/record"
    deliver_after = datetime.now(timezone.utc) + timedelta(seconds=1.5)
    first = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": target,
        "payload": {},
        "deliver_after": deliver_after.isoformat(),
    })
    first_id = first.json()["id"]
    second = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": target,
        "payload": {},
    })
    second_id = second.json()["id"]

    # The second event has no delay of its own but must not overtake the
    # not-yet-due head event.
    time.sleep(1.0)
    assert _get_event(server.base_url, first_id)["status"] == "queued"
    second_e = _get_event(server.base_url, second_id)
    assert second_e["status"] == "queued"
    assert second_e["attempts_count"] == 0

    wait_for(lambda: _get_event(server.base_url, first_id)["status"] == "delivered")
    wait_for(lambda: _get_event(server.base_url, second_id)["status"] == "delivered")
    t1 = _parse_iso(_get_event(server.base_url, first_id)["delivered_at"])
    t2 = _parse_iso(_get_event(server.base_url, second_id)["delivered_at"])
    assert deliver_after <= t1 <= t2


def test_invalid_delivery_window_returns_422(server):
    now = datetime.now(timezone.utc)
    base = {
        "idempotency_key": uuid.uuid4().hex,
        "target": f"{server.base_url}/t/record",
        "payload": {},
    }
    # expires_at before deliver_after.
    resp = _post_event_obj(server.base_url, base | {
        "deliver_after": (now + timedelta(seconds=10)).isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
    })
    assert resp.status_code == 422
    # Equal is not "later".
    same = (now + timedelta(seconds=10)).isoformat()
    resp = _post_event_obj(
        server.base_url, base | {"deliver_after": same, "expires_at": same}
    )
    assert resp.status_code == 422
    # Not RFC 3339 at all / missing timezone offset.
    resp = _post_event_obj(server.base_url, base | {"deliver_after": "not-a-date"})
    assert resp.status_code == 422
    resp = _post_event_obj(
        server.base_url, base | {"expires_at": "2030-01-01 00:00:00"}
    )
    assert resp.status_code == 422
    # Nothing was stored for any of those attempts.
    assert (
        httpx.get(f"{server.base_url}/events/key/{base['idempotency_key']}").status_code
        == 404
    )


def test_duplicate_key_returns_original_despite_window_fields(server):
    key = uuid.uuid4().hex
    r1 = _post_event_obj(server.base_url, {
        "idempotency_key": key,
        "target": f"{server.base_url}/t/record",
        "payload": {},
        "deliver_after": (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat(),
    })
    assert r1.status_code == 201

    # Same key with a different (valid) window: the stored event wins.
    r2 = _post_event_obj(server.base_url, {
        "idempotency_key": key,
        "target": f"{server.base_url}/t/record",
        "payload": {},
        "deliver_after": (datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat(),
        "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=50)).isoformat(),
    })
    assert r2.status_code == 200
    assert r2.headers.get("idempotent-replay") == "true"
    assert r2.json()["id"] == r1.json()["id"]
    assert r2.json()["deliver_after"] == r1.json()["deliver_after"]
    assert r2.json()["expires_at"] is None

    # Even an invalid window on a duplicate key yields the original event.
    now = datetime.now(timezone.utc)
    r3 = _post_event_obj(server.base_url, {
        "idempotency_key": key,
        "target": f"{server.base_url}/t/record",
        "payload": {},
        "deliver_after": (now + timedelta(seconds=10)).isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
    })
    assert r3.status_code == 200
    assert r3.json()["id"] == r1.json()["id"]

    wait_for(
        lambda: _get_event(server.base_url, r1.json()["id"])["status"] == "delivered"
    )
    mine = [r for r in receipts if r["idempotency_key"] == key]
    assert len(mine) == 1  # exactly one delivery ever


def test_queued_event_expires_without_attempt_and_unblocks_next(server):
    from .conftest import _gates
    token = f"expire-{uuid.uuid4().hex}"
    _gates[token] = threading.Event()

    first = _post_event(server.base_url, "/t/block", extra_query=f"?token={token}")
    first_id = first.json()["id"]
    wait_for(
        lambda: _get_event(server.base_url, first_id)["status"] == "processing",
        timeout=10,
    )

    # The middle event is queued behind the blocked head and its window
    # closes while it waits.
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=1.0)
    second = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": f"{server.base_url}/t/block?token={token}",
        "payload": {},
        "expires_at": expires_at.isoformat(),
    })
    assert second.status_code == 201
    second_id = second.json()["id"]
    assert second.json()["expires_at"] is not None

    third = _post_event(server.base_url, "/t/block", extra_query=f"?token={token}")
    third_id = third.json()["id"]

    try:
        expired = wait_for(
            lambda: (lambda e: e if e["status"] == "expired" else None)(
                _get_event(server.base_url, second_id)
            ),
            timeout=10,
        )
        # Expired while queued: no attempt was ever made.
        assert expired["attempts_count"] == 0
        assert expired["attempts"] == []
        # The head is still in flight, so the third event stays queued.
        assert _get_event(server.base_url, third_id)["status"] == "queued"
    finally:
        httpx.post(f"{server.base_url}/t/release", params={"token": token})

    # The expired event is skipped; the queue continues with the third.
    wait_for(lambda: _get_event(server.base_url, first_id)["status"] == "delivered")
    wait_for(lambda: _get_event(server.base_url, third_id)["status"] == "delivered")
    final_second = _get_event(server.base_url, second_id)
    assert final_second["status"] == "expired"
    assert final_second["attempts_count"] == 0
    assert [r for r in receipts if r["event_id"] == second_id] == []

    # The expired status is filterable and carries the window fields.
    listed = httpx.get(
        f"{server.base_url}/events", params={"status": "expired"}
    ).json()
    entry = [e for e in listed["events"] if e["id"] == second_id]
    assert entry and entry[0]["expires_at"] is not None
    assert entry[0]["deliver_after"]


def test_processing_event_is_not_interrupted_by_expiry(server):
    from .conftest import _gates
    token = f"expire-inflight-{uuid.uuid4().hex}"
    _gates[token] = threading.Event()

    resp = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": f"{server.base_url}/t/block?token={token}",
        "payload": {},
        "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=1.5)).isoformat(),
    })
    event_id = resp.json()["id"]
    wait_for(
        lambda: _get_event(server.base_url, event_id)["status"] == "processing",
        timeout=10,
    )
    try:
        # The deadline passes while the attempt is in flight: no interruption.
        time.sleep(2.0)
        assert _get_event(server.base_url, event_id)["status"] == "processing"
    finally:
        httpx.post(f"{server.base_url}/t/release", params={"token": token})

    final = wait_for(
        lambda: (lambda e: e if e["status"] == "delivered" else None)(
            _get_event(server.base_url, event_id)
        ),
        timeout=10,
    )
    assert final["attempts_count"] == 1


# -------------------------------------------------------------- batch ingest

def _post_batch(base_url: str, items, sign: bool = True) -> httpx.Response:
    body = json.dumps(items).encode()
    headers = {"Content-Type": "application/json"}
    if sign:
        headers["X-Webhook-Signature"] = _sign(body)
    return httpx.post(f"{base_url}/events/batch", content=body, headers=headers)


def _batch_item(base_url: str, key: str | None = None,
                target_path: str = "/t/record", **extra) -> dict:
    item = {
        "idempotency_key": key or uuid.uuid4().hex,
        "target": f"{base_url}{target_path}",
        "payload": {"v": 1},
    }
    item.update(extra)
    return item


def test_batch_requires_valid_signature(server):
    items = [_batch_item(server.base_url)]
    assert _post_batch(server.base_url, items, sign=False).status_code == 401
    body = json.dumps(items).encode()
    resp = httpx.post(
        f"{server.base_url}/events/batch",
        content=body,
        headers={"X-Webhook-Signature": "sha256=deadbeef"},
    )
    assert resp.status_code == 401
    # Signature over a different (tampered) body is rejected too.
    resp = httpx.post(
        f"{server.base_url}/events/batch",
        content=body,
        headers={"X-Webhook-Signature": _sign(body + b" ")},
    )
    assert resp.status_code == 401


def test_batch_rejects_non_array_and_non_json_body(server):
    body = b'{"idempotency_key":"x"}'
    resp = httpx.post(
        f"{server.base_url}/events/batch",
        content=body,
        headers={"X-Webhook-Signature": _sign(body)},
    )
    assert resp.status_code == 400
    body = b"not-json"
    resp = httpx.post(
        f"{server.base_url}/events/batch",
        content=body,
        headers={"X-Webhook-Signature": _sign(body)},
    )
    assert resp.status_code == 400


def test_batch_size_limits(server):
    assert _post_batch(server.base_url, []).status_code == 422
    too_many = [_batch_item(server.base_url) for _ in range(101)]
    assert _post_batch(server.base_url, too_many).status_code == 422
    # The upper bound itself is accepted. A dedicated target URL keeps this
    # 100-event queue from slowing down the other tests' targets.
    target_path = f"/dev/sink?sizetest={uuid.uuid4().hex}"
    exactly = [
        _batch_item(server.base_url, target_path=target_path) for _ in range(100)
    ]
    resp = _post_batch(server.base_url, exactly)
    assert resp.status_code == 201
    assert len(resp.json()["events"]) == 100


def test_batch_item_shape_errors(server):
    assert _post_batch(server.base_url, [42]).status_code == 422
    assert _post_batch(server.base_url, [{"target": "http://x"}]).status_code == 422
    item = _batch_item(server.base_url)
    item["idempotency_key"] = ""
    assert _post_batch(server.base_url, [item]).status_code == 422
    item = _batch_item(server.base_url)
    item["idempotency_key"] = 123
    assert _post_batch(server.base_url, [item]).status_code == 422


def test_batch_duplicate_keys_within_batch_rejected(server):
    key = f"batchdup-{uuid.uuid4().hex}"
    items = [
        _batch_item(server.base_url, key=key),
        _batch_item(server.base_url),
        _batch_item(server.base_url, key=key),
    ]
    resp = _post_batch(server.base_url, items)
    assert resp.status_code == 422
    assert key in resp.json()["detail"]
    # Nothing from the batch was stored.
    for item in items:
        assert (
            httpx.get(
                f"{server.base_url}/events/key/{item['idempotency_key']}"
            ).status_code
            == 404
        )


def test_batch_creates_and_delivers_in_array_order(server):
    # A dedicated target URL (query strings make distinct targets) gives this
    # batch its own head-of-queue, so delivery order reflects only this batch.
    target_path = f"/t/record?ord={uuid.uuid4().hex}"
    items = [_batch_item(server.base_url, target_path=target_path) for _ in range(3)]
    resp = _post_batch(server.base_url, items)
    assert resp.status_code == 201
    assert "idempotent-replay" not in resp.headers
    out = resp.json()["events"]
    assert [e["created"] for e in out] == [True, True, True]
    ids = [e["event"]["id"] for e in out]
    assert len(set(ids)) == 3
    # Response order matches input order (keys echo back in place).
    assert [e["event"]["idempotency_key"] for e in out] == [
        i["idempotency_key"] for i in items
    ]

    for event_id in ids:
        wait_for(lambda: _get_event(server.base_url, event_id)["status"] == "delivered")
    # Same target => strict head-of-queue order: deliveries follow array order.
    mine = [r for r in receipts if r["event_id"] in ids]
    assert [r["event_id"] for r in mine] == ids


def test_batch_existing_key_returns_original_and_ignores_other_fields(server):
    key_a = f"batch-existing-{uuid.uuid4().hex}"
    first = _post_event(server.base_url, "/t/record", key=key_a)
    assert first.status_code == 201
    original = first.json()

    now = datetime.now(timezone.utc)
    dup = {
        "idempotency_key": key_a,
        "target": f"{server.base_url}/t/record",
        "payload": {"changed": True},
        # Even an invalid window on a known key is ignored entirely.
        "deliver_after": (now + timedelta(seconds=10)).isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
    }
    new_item = _batch_item(server.base_url)
    resp = _post_batch(server.base_url, [dup, new_item])
    assert resp.status_code == 200
    assert resp.headers.get("idempotent-replay") == "true"
    out = resp.json()["events"]
    assert out[0]["created"] is False
    assert out[0]["event"]["id"] == original["id"]
    assert out[0]["event"]["payload"] == original["payload"]
    assert out[0]["event"]["expires_at"] is None
    assert out[1]["created"] is True

    wait_for(lambda: _get_event(server.base_url, original["id"])["status"] == "delivered")
    wait_for(
        lambda: _get_event(server.base_url, out[1]["event"]["id"])["status"]
        == "delivered"
    )
    # The duplicate item did not trigger a second delivery of the original.
    mine = [r for r in receipts if r["idempotency_key"] == key_a]
    assert len(mine) == 1


def test_batch_invalid_new_event_rolls_back_whole_batch(server):
    good_1 = _batch_item(server.base_url)
    bad_target = _batch_item(server.base_url, target_path="/t/record")
    bad_target["target"] = "not-a-url"
    good_2 = _batch_item(server.base_url)
    resp = _post_batch(server.base_url, [good_1, bad_target, good_2])
    assert resp.status_code == 422
    # No new event of the batch was stored, including the valid ones.
    for item in (good_1, good_2):
        assert (
            httpx.get(
                f"{server.base_url}/events/key/{item['idempotency_key']}"
            ).status_code
            == 404
        )

    # Same all-or-nothing rule when the delivery window is invalid.
    now = datetime.now(timezone.utc)
    good_3 = _batch_item(server.base_url)
    bad_window = _batch_item(
        server.base_url,
        deliver_after=(now + timedelta(seconds=10)).isoformat(),
        expires_at=(now + timedelta(seconds=5)).isoformat(),
    )
    resp = _post_batch(server.base_url, [good_3, bad_window])
    assert resp.status_code == 422
    assert (
        httpx.get(
            f"{server.base_url}/events/key/{good_3['idempotency_key']}"
        ).status_code
        == 404
    )


def test_batch_resend_never_delivers_twice(server):
    items = [_batch_item(server.base_url) for _ in range(2)]
    first = _post_batch(server.base_url, items)
    assert first.status_code == 201
    ids = [e["event"]["id"] for e in first.json()["events"]]
    for event_id in ids:
        wait_for(lambda: _get_event(server.base_url, event_id)["status"] == "delivered")

    second = _post_batch(server.base_url, items)
    assert second.status_code == 200
    assert second.headers.get("idempotent-replay") == "true"
    out = second.json()["events"]
    assert [e["created"] for e in out] == [False, False]
    assert [e["event"]["id"] for e in out] == ids

    time.sleep(0.5)
    for item in items:
        mine = [r for r in receipts if r["idempotency_key"] == item["idempotency_key"]]
        assert len(mine) == 1  # exactly one outbound delivery per event ever


def test_batch_events_queue_behind_existing_head(server):
    from .conftest import _gates
    token = f"batch-order-{uuid.uuid4().hex}"
    _gates[token] = threading.Event()

    head = _post_event(server.base_url, "/t/block", extra_query=f"?token={token}")
    head_id = head.json()["id"]
    wait_for(
        lambda: _get_event(server.base_url, head_id)["status"] == "processing",
        timeout=10,
    )

    items = [
        _batch_item(server.base_url, target_path=f"/t/block?token={token}")
        for _ in range(2)
    ]
    resp = _post_batch(server.base_url, items)
    assert resp.status_code == 201
    batch_ids = [e["event"]["id"] for e in resp.json()["events"]]
    try:
        time.sleep(0.5)
        # The blocked head keeps the whole batch queued, in order.
        for event_id in batch_ids:
            event = _get_event(server.base_url, event_id)
            assert event["status"] == "queued"
            assert event["attempts_count"] == 0
    finally:
        httpx.post(f"{server.base_url}/t/release", params={"token": token})

    wait_for(lambda: _get_event(server.base_url, head_id)["status"] == "delivered")
    for event_id in batch_ids:
        wait_for(lambda: _get_event(server.base_url, event_id)["status"] == "delivered")
    times = [
        _parse_iso(_get_event(server.base_url, eid)["delivered_at"])
        for eid in [head_id, *batch_ids]
    ]
    assert times[0] <= times[1] <= times[2]

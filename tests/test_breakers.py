"""End-to-end tests for the persistent per-target circuit breaker."""
from __future__ import annotations

import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import httpx

from app import db
from tests.test_webhooks import _get_event, _post_event_obj
from .conftest import wait_for


def _breaker(base_url: str, target: str) -> dict | None:
    rows = httpx.get(
        f"{base_url}/targets/breakers", params={"target": target}
    ).json()["breakers"]
    return rows[0] if rows else None


def _wait_breaker(base_url: str, target: str, state: str, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = _breaker(base_url, target)
        if last is not None and last["state"] == state:
            return last
        time.sleep(0.05)
    raise AssertionError(f"breaker for {target} never reached {state}; last={last!r}")


def _release(base_url: str, gate: str) -> None:
    httpx.post(f"{base_url}/t/probe-release", params={"gate": gate})


# ---------------------------------------------------- open -> half-open -> close

def test_breaker_opens_after_three_failures_and_closes_on_probe_success(server):
    gate = f"brk-{uuid.uuid4().hex}"
    target = f"{server.base_url}/t/probe?fail_times=3&gate={gate}"
    try:
        first = _post_event_obj(server.base_url, {
            "idempotency_key": uuid.uuid4().hex,
            "target": target,
            "payload": {},
        }).json()
        first_id = first["id"]
        # A second, independently-created event waits behind the head.
        second = _post_event_obj(server.base_url, {
            "idempotency_key": uuid.uuid4().hex,
            "target": target,
            "payload": {},
        }).json()
        second_id = second["id"]

        # Three consecutive failures (attempts at t≈0, t≈1, t≈3) open the
        # breaker; the 4th attempt of the round must NOT fire yet.
        opened = _wait_breaker(server.base_url, target, "open")
        assert opened["consecutive_failures"] == 3
        assert opened["probe_event_id"] is None
        # open_until is a parseable RFC 3339 deadline still in the future.
        assert opened["open_until"] is not None
        assert datetime.fromisoformat(opened["open_until"]) > datetime.now(timezone.utc)
        assert _get_event(server.base_url, first_id)["attempts_count"] == 3
        assert _get_event(server.base_url, second_id)["attempts_count"] == 0

        # While open, no event for the target is claimable even though the
        # head's 4th backoff (4s) is not yet due either; once the breaker
        # deadline elapses the due head becomes the single half-open probe
        # and blocks on the gate (4th call -> block).
        probe = _wait_breaker(server.base_url, target, "half-open", timeout=10)
        assert probe["probe_event_id"] == first_id
        assert probe["open_until"] is None
        assert probe["consecutive_failures"] == 3
        assert _get_event(server.base_url, first_id)["status"] == "processing"
        # The second event must never overtake the held probe.
        time.sleep(0.4)
        assert _get_event(server.base_url, second_id)["attempts_count"] == 0

        # Successful probe: breaker closes, counter zeroed, queue drains.
        _release(server.base_url, gate)
        closed = _wait_breaker(server.base_url, target, "closed", timeout=10)
        assert closed["consecutive_failures"] == 0
        assert closed["open_until"] is None
        assert closed["probe_event_id"] is None
        wait_for(lambda: _get_event(server.base_url, first_id)["status"] == "delivered")
        wait_for(lambda: _get_event(server.base_url, second_id)["status"] == "delivered")
    finally:
        _release(server.base_url, gate)


# ------------------------------------------------ failed probe reopens the breaker

def test_failed_half_open_probe_reopens_breaker(server):
    target = f"{server.base_url}/t/probe?fail_times=10"
    first = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": target,
        "payload": {},
    }).json()
    first_id = first["id"]
    queued = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": target,
        "payload": {},
    }).json()
    queued_id = queued["id"]

    _wait_breaker(server.base_url, target, "open")
    assert _get_event(server.base_url, first_id)["attempts_count"] == 3

    # Open window elapses: the head goes as the half-open probe and fails
    # again (its 4th failed round attempt -> dead) and the breaker reopens
    # for another full window with the failure count at 4.
    deadline = time.time() + 10
    reopened = None
    while time.time() < deadline:
        reopened = _breaker(server.base_url, target)
        if (reopened and reopened["state"] == "open"
                and reopened["consecutive_failures"] == 4):
            break
        time.sleep(0.05)
    assert reopened is not None
    assert reopened["state"] == "open"
    assert reopened["consecutive_failures"] == 4
    assert reopened["probe_event_id"] is None
    assert reopened["open_until"] is not None

    # The 4th failed attempt exhausted the round; nothing for the target is
    # claimable during the fresh open window and the queued event stays put.
    event = _get_event(server.base_url, first_id)
    assert event["status"] == "dead"
    assert event["attempts_count"] == 4
    assert _get_event(server.base_url, queued_id)["attempts_count"] == 0


# --------------------------------------------- other targets stay deliverable

def test_open_breaker_does_not_block_other_targets(server):
    bad = f"{server.base_url}/t/probe?fail_times=10&tag={uuid.uuid4().hex}"
    bad_event = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": bad,
        "payload": {},
    }).json()

    # While the bad target trips open, a good target delivers immediately.
    good = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": f"{server.base_url}/t/record",
        "payload": {},
    }).json()
    wait_for(lambda: _get_event(server.base_url, good["id"])["status"] == "delivered")

    opened = _wait_breaker(server.base_url, bad, "open")
    assert opened["consecutive_failures"] == 3
    # Another healthy event to another target still goes straight out.
    good2 = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": f"{server.base_url}/t/record",
        "payload": {},
    }).json()
    wait_for(lambda: _get_event(server.base_url, good2["id"])["status"] == "delivered")
    assert _get_event(server.base_url, bad_event["id"])["status"] in ("queued", "processing")


# ---------------------------------------------------------- query API filtering

def test_breakers_endpoint_filters_and_reports_shape(server):
    target = f"{server.base_url}/t/probe?fail_times=3&shape={uuid.uuid4().hex}"
    _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": target,
        "payload": {},
    })
    _wait_breaker(server.base_url, target, "open")

    resp = httpx.get(f"{server.base_url}/targets/breakers")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {"breakers", "limit", "offset"}
    row = [b for b in body["breakers"] if b["target"] == target][0]
    assert set(row) == {
        "target", "state", "consecutive_failures", "open_until", "probe_event_id",
    }
    assert row["state"] == "open"

    by_state = httpx.get(
        f"{server.base_url}/targets/breakers", params={"state": "open"}
    ).json()["breakers"]
    assert all(b["state"] == "open" for b in by_state)
    assert any(b["target"] == target for b in by_state)

    by_target = httpx.get(
        f"{server.base_url}/targets/breakers", params={"target": target}
    ).json()["breakers"]
    assert [b["target"] for b in by_target] == [target]

    # Closed rows are reported too.
    closed_target = f"{server.base_url}/t/record?closed={uuid.uuid4().hex}"
    _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": closed_target,
        "payload": {},
    })
    wait_for(
        lambda: httpx.get(
            f"{server.base_url}/targets/breakers", params={"target": closed_target}
        ).json()["breakers"]
    )
    closed_rows = httpx.get(
        f"{server.base_url}/targets/breakers", params={"state": "closed"}
    ).json()["breakers"]
    assert any(b["target"] == closed_target and b["consecutive_failures"] == 0
               for b in closed_rows)

    # Invalid state filter is a 422; unknown target is an empty 200 list.
    assert httpx.get(
        f"{server.base_url}/targets/breakers", params={"state": "nope"}
    ).status_code == 422
    assert httpx.get(
        f"{server.base_url}/targets/breakers",
        params={"target": "http://does-not-exist.invalid/x"},
    ).json()["breakers"] == []


# ------------------------------------------------ persistence across restart

def test_breaker_state_survives_restart_recovery(server):
    base = server.base_url
    open_gate = f"persist-open-{uuid.uuid4().hex}"
    half_gate = f"persist-half-{uuid.uuid4().hex}"
    # Both targets fail 3 times then block on a gate on every later call, so
    # the live worker cannot race the recovery assertions below.
    open_target = f"{base}/t/probe?fail_times=3&gate={open_gate}"
    half_target = f"{base}/t/probe?fail_times=3&gate={half_gate}"
    try:
        _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex,
            "target": open_target,
            "payload": {},
        })
        _wait_breaker(base, open_target, "open")

        half_id = _post_event_obj(base, {
            "idempotency_key": uuid.uuid4().hex,
            "target": half_target,
            "payload": {},
        }).json()["id"]
        _wait_breaker(base, half_target, "open")

        # Simulate a crash mid-probe for half_target, while open_target sits
        # in a normal open window. Push the open deadline far into the future
        # so the live worker stays away while the recovery is inspected.
        far_future = time.time() + 3600
        with db.db_lock:
            conn = db.get_conn()
            conn.execute(
                "UPDATE breakers SET state = 'half-open', probe_event_id = ?, "
                "open_until_ts = NULL WHERE target = ?",
                (half_id, half_target),
            )
            conn.execute(
                "UPDATE events SET status = 'processing' WHERE id = ?", (half_id,)
            )
            conn.execute(
                "UPDATE breakers SET open_until_ts = ? WHERE target = ?",
                (far_future, open_target),
            )
            conn.commit()
            open_snapshot = db.list_breakers(target=open_target)[0]

            # Exactly what application startup runs. Holding db_lock here
            # also means the worker cannot claim anything in between.
            db.reset_stuck_events()

            open_row = db.list_breakers(target=open_target)[0]
            assert open_row["state"] == "open"
            # An untouched open window keeps counting down across the
            # restart: deadline and counter are byte-for-byte unchanged.
            assert open_row["open_until"] == open_snapshot["open_until"]
            assert open_row["consecutive_failures"] == 3
            assert open_row["probe_event_id"] is None

            half_row = db.list_breakers(target=half_target)[0]
            assert half_row["state"] == "open"
            assert half_row["probe_event_id"] is None
            assert half_row["consecutive_failures"] == 3
            assert half_row["open_until"] is not None
            assert datetime.fromisoformat(half_row["open_until"]) <= datetime.now(
                timezone.utc
            )
            # Direct db read (an HTTP call here would block on db_lock).
            assert db.get_event(half_id)["status"] == "queued"

        # Recovered head goes out again as the half-open probe and blocks on
        # the gate; releasing it returns 200, closing the breaker.
        _wait_breaker(base, half_target, "half-open")
        _release(base, half_gate)
        _wait_breaker(base, half_target, "closed")
        wait_for(lambda: _get_event(base, half_id)["status"] == "delivered")
    finally:
        _release(base, open_gate)
        _release(base, half_gate)


# ---------------------------------------------------- production defaults exist

def test_default_breaker_settings():
    out = subprocess.run(
        [
            sys.executable, "-c",
            "from app import config; "
            "print(config.BREAKER_FAILURE_THRESHOLD, config.BREAKER_OPEN_SECONDS)",
        ],
        cwd="/workspace",
        env={
            "PATH": "/usr/bin:/bin",
            "WEBHOOK_SECRET": "x",
            "WEBHOOK_DB_PATH": "/tmp/unused-defaults-check.db",
        },
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == "3 10.0"

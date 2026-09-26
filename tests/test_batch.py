"""End-to-end tests for the batch ingestion endpoint ``POST /events/batch``."""
from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi import Request
from fastapi.responses import JSONResponse

from .conftest import wait_for
from app.main import app as main_app

SECRET = "test-secret"
BATCH_SINK = "/t/batch-record"

# Batch tests enqueue up to 100 events at a time and share a session-scoped
# server with the other suites. They therefore use their own target sink and
# receipt log so they can neither crowd the default list window (limit 50)
# nor mix into the other tests' ``receipts`` assertions.
_batch_receipts: list[dict] = []
_batch_sink_registered = False


@pytest.fixture(autouse=True)
def _batch_sink():
    global _batch_sink_registered
    if not _batch_sink_registered:

        @main_app.post(BATCH_SINK, include_in_schema=False)
        async def _t_batch_record(request: Request):
            body = await request.body()
            _batch_receipts.append(
                {
                    "event_id": request.headers.get("x-event-id"),
                    "idempotency_key": request.headers.get("x-idempotency-key"),
                    "body": body.decode(),
                }
            )
            return JSONResponse({"ok": True})

        _batch_sink_registered = True
    yield
    # Remove every event aimed at the batch sink (and its attempts) so the
    # up-to-100 rows can never crowd the shared list window used by the
    # other suites, which share this session's database.
    from app import db

    with db.db_lock:
        conn = db.get_conn()
        conn.execute(
            "DELETE FROM attempts WHERE event_id IN "
            "(SELECT id FROM events WHERE target LIKE ?)",
            (f"%{BATCH_SINK}",),
        )
        conn.execute("DELETE FROM events WHERE target LIKE ?", (f"%{BATCH_SINK}",))
        conn.commit()
    _batch_receipts.clear()


def _sign(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


def _post_batch(base_url: str, items: list[dict]) -> httpx.Response:
    body = json.dumps(items).encode()
    return httpx.post(
        f"{base_url}/events/batch",
        content=body,
        headers={"X-Webhook-Signature": _sign(body), "Content-Type": "application/json"},
    )


def _post_batch_raw(base_url: str, raw: bytes, signature: str | None = None) -> httpx.Response:
    headers = {"Content-Type": "application/json"}
    if signature is not None:
        headers["X-Webhook-Signature"] = signature
    return httpx.post(f"{base_url}/events/batch", content=raw, headers=headers)


def _post_event_single(base_url: str, key: str, payload=None) -> httpx.Response:
    body = (
        b'{"idempotency_key":"' + key.encode()
        + b'","target":"' + f"{base_url}{BATCH_SINK}".encode() + b'"'
        + b',"payload":' + (payload or b'{"v":1}') + b"}"
    )
    return httpx.post(
        f"{base_url}/events",
        content=body,
        headers={"X-Webhook-Signature": _sign(body)},
    )


def _item(base_url: str, key: str, payload=None, **extra) -> dict:
    item = {
        "idempotency_key": key,
        "target": f"{base_url}{BATCH_SINK}",
        "payload": payload if payload is not None else {},
    }
    item.update(extra)
    return item


def _get_event(base_url: str, event_id: str) -> dict:
    return httpx.get(f"{base_url}/events/{event_id}").json()


# ------------------------------------------------------------- signature

def test_batch_requires_signature_over_whole_raw_body(server):
    items = [_item(server.base_url, f"no-sig-{uuid.uuid4().hex}")]
    assert _post_batch_raw(server.base_url, json.dumps(items).encode()).status_code == 401
    # A signature computed for a different body must not validate.
    body = json.dumps(items).encode()
    resp = _post_batch_raw(server.base_url, body, _sign(body + b" "))
    assert resp.status_code == 401


def test_batch_body_shape_errors(server):
    # Non-array body -> 400 (signature is over that exact body).
    resp = _post_batch_raw(server.base_url, b"{}", _sign(b"{}"))
    assert resp.status_code == 400
    assert "array" in resp.json()["detail"]

    # Empty / oversize arrays -> 422, nothing stored.
    assert _post_batch_raw(server.base_url, b"[]", _sign(b"[]")).status_code == 422
    too_big = json.dumps(
        [_item(server.base_url, f"big-{i}") for i in range(101)]
    ).encode()
    assert _post_batch_raw(server.base_url, too_big, _sign(too_big)).status_code == 422


# ------------------------------------------------------------- happy path

def test_batch_enqueues_new_events_in_order(server):
    keys = [f"batch-{uuid.uuid4().hex}-{i}" for i in range(3)]
    items = [_item(server.base_url, key, {"i": i}) for i, key in enumerate(keys)]
    resp = _post_batch(server.base_url, items)
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) == 3
    assert [r["created"] for r in results] == [True, True, True]
    # Input order is preserved.
    assert [r["event"]["idempotency_key"] for r in results] == keys
    event_ids = [r["event"]["id"] for r in results]
    for result in results:
        assert result["event"]["status"] in ("queued", "processing")
        assert "attempts" in result["event"]

    for event_id in event_ids:
        wait_for(lambda eid=event_id: _get_event(server.base_url, eid)["status"] == "delivered")

    # Exactly one outbound delivery per key, in array order (same target).
    mine = [r for r in _batch_receipts if r["idempotency_key"] in keys]
    assert [r["idempotency_key"] for r in mine] == keys
    assert len(mine) == 3


def test_batch_resend_returns_originals_without_duplicate_delivery(server):
    keys = [f"resend-{uuid.uuid4().hex}-{i}" for i in range(2)]
    items = [_item(server.base_url, key, {"v": 1}) for key in keys]
    r1 = _post_batch(server.base_url, items)
    assert [x["created"] for x in r1.json()["results"]] == [True, True]
    first_ids = [x["event"]["id"] for x in r1.json()["results"]]
    for event_id in first_ids:
        wait_for(lambda eid=event_id: _get_event(server.base_url, eid)["status"] == "delivered")

    # Resubmit the very same batch, tweaking payload/target fields: the
    # stored originals must come back, created=false, fields ignored.
    changed = [
        _item(server.base_url, keys[0], {"v": 999}),
        {
            "idempotency_key": keys[1],
            "target": "http://127.0.0.1:1/nope",
            "payload": {"v": 999},
        },
    ]
    r2 = _post_batch(server.base_url, changed)
    assert r2.status_code == 200
    results = r2.json()["results"]
    assert [x["created"] for x in results] == [False, False]
    assert [x["event"]["id"] for x in results] == first_ids
    assert results[0]["event"]["payload"] == {"v": 1}
    assert results[1]["event"]["target"] == f"{server.base_url}{BATCH_SINK}"

    # No second delivery was ever enqueued for either key.
    def count():
        return len([r for r in _batch_receipts if r["idempotency_key"] in keys])

    assert count() == 2


def test_batch_mixes_existing_and_new_events(server):
    existing_key = f"mix-old-{uuid.uuid4().hex}"
    created = _post_event_single(server.base_url, existing_key, b'{"v":"old"}')
    assert created.status_code == 201
    existing_id = created.json()["id"]
    wait_for(
        lambda: _get_event(server.base_url, existing_id)["status"] == "delivered"
    )

    new_key = f"mix-new-{uuid.uuid4().hex}"
    resp = _post_batch(
        server.base_url,
        [
            _item(server.base_url, existing_key, {"v": "ignored"}),
            _item(server.base_url, new_key, {"v": "new"}),
        ],
    )
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert [x["created"] for x in results] == [False, True]
    assert results[0]["event"]["id"] == existing_id
    assert results[0]["event"]["payload"] == {"v": "old"}
    assert results[1]["event"]["idempotency_key"] == new_key
    wait_for(
        lambda: _get_event(server.base_url, results[1]["event"]["id"])["status"]
        == "delivered"
    )


# ------------------------------------------------------------- batch validation

def test_duplicate_keys_within_batch_return_422_and_store_nothing(server):
    dup = f"inbatch-dup-{uuid.uuid4().hex}"
    other = f"inbatch-other-{uuid.uuid4().hex}"
    resp = _post_batch(
        server.base_url,
        [
            _item(server.base_url, dup),
            _item(server.base_url, other),
            _item(server.base_url, dup),
        ],
    )
    assert resp.status_code == 422
    assert dup in resp.json()["detail"]
    # The whole batch is rejected, including the once-seen 'other' key.
    assert httpx.get(f"{server.base_url}/events/key/{dup}").status_code == 404
    assert httpx.get(f"{server.base_url}/events/key/{other}").status_code == 404


def test_invalid_items_report_their_index(server):
    key = f"idx-{uuid.uuid4().hex}"
    body = json.dumps(
        [
            _item(server.base_url, key),
            "not-an-object",
            _item(server.base_url, f"idx-bad-{uuid.uuid4().hex}") | {"target": "no-url"},
        ]
    ).encode()
    resp = _post_batch_raw(server.base_url, body, _sign(body))
    assert resp.status_code == 422
    locs = [tuple(err["loc"]) for err in resp.json()["detail"]]
    assert (1,) in locs
    assert any(loc[:2] == (2, "target") for loc in locs)
    assert httpx.get(f"{server.base_url}/events/key/{key}").status_code == 404


def test_invalid_window_rejects_and_rolls_back_entire_batch(server):
    now = datetime.now(timezone.utc)
    good = f"rollback-good-{uuid.uuid4().hex}"
    bad = f"rollback-bad-{uuid.uuid4().hex}"
    resp = _post_batch(
        server.base_url,
        [
            _item(server.base_url, good),
            _item(
                server.base_url,
                bad,
                deliver_after=(now + timedelta(seconds=10)).isoformat(),
                expires_at=(now + timedelta(seconds=5)).isoformat(),
            ),
        ],
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail["index"] == 1
    # None of this batch's new events made it to the database.
    assert httpx.get(f"{server.base_url}/events/key/{good}").status_code == 404
    assert httpx.get(f"{server.base_url}/events/key/{bad}").status_code == 404


def test_existing_key_with_invalid_window_still_returns_original(server):
    key = f"existing-window-{uuid.uuid4().hex}"
    r1 = _post_event_single(server.base_url, key)
    assert r1.status_code == 201
    original_id = r1.json()["id"]
    now = datetime.now(timezone.utc)
    resp = _post_batch(
        server.base_url,
        [
            _item(
                server.base_url,
                key,
                deliver_after=(now + timedelta(seconds=10)).isoformat(),
                expires_at=(now + timedelta(seconds=5)).isoformat(),
            )
        ],
    )
    assert resp.status_code == 200
    result = resp.json()["results"][0]
    assert result["created"] is False
    assert result["event"]["id"] == original_id


def test_batch_accepts_up_to_100_events(server):
    keys = [f"hundred-{uuid.uuid4().hex}-{i}" for i in range(100)]
    resp = _post_batch(server.base_url, [_item(server.base_url, k) for k in keys])
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) == 100
    assert all(r["created"] for r in results)

"""FastAPI application: reliable webhook ingestion, delivery and query API."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, BaseModel, Field, HttpUrl, ValidationError

from . import config, db
from .worker import Worker

logger = logging.getLogger("webhook.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class EventIn(BaseModel):
    idempotency_key: str = Field(min_length=1)
    target: HttpUrl
    payload: Any = Field(default_factory=dict)
    # Optional delivery window, RFC 3339 (timezone offset required).
    # deliver_after defaults to the creation moment; expires_at, when
    # present, must be strictly later than deliver_after.
    deliver_after: AwareDatetime | None = None
    expires_at: AwareDatetime | None = None


# In-memory counters for the built-in demo sink (not part of the core API).
_sink_lock = threading.Lock()
_sink_calls: dict[str, int] = {}


def _public_event(event: dict) -> dict:
    """Strip internal fields and expose the public event representation."""
    return {
        "id": event["id"],
        "idempotency_key": event["idempotency_key"],
        "target": event["target"],
        "payload": event["payload"],
        "status": event["status"],
        "attempts_count": event["attempts_count"],
        "created_at": event["created_at"],
        "deliver_after": event.get("deliver_after"),
        "expires_at": event.get("expires_at"),
        "delivered_at": event.get("delivered_at"),
        "replay_count": event.get("replay_count", 0),
        "last_replayed_at": event.get("last_replayed_at"),
        "attempts": event.get("attempts", []),
    }


def _valid_signature(body: bytes, signature: str | None) -> bool:
    if not signature:
        return False
    expected = "sha256=" + hmac.new(
        config.WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    # Tolerate clients sending the bare hex digest as well.
    provided = signature if signature.startswith("sha256=") else f"sha256={signature}"
    return hmac.compare_digest(provided, expected)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    # Recover deliveries interrupted while this process was restarted.
    db.reset_stuck_events()
    worker = Worker()
    worker.start()
    app.state.worker = worker
    try:
        yield
    finally:
        worker.stop()


app = FastAPI(
    title="Reliable Webhook Delivery API",
    version="1.4.0",
    description="Sign, enqueue, reliably deliver, replay and inspect webhook events, "
    "with persistent per-target circuit breakers.",
    lifespan=lifespan,
)


# Maximum number of events accepted by one POST /events/batch call.
MAX_BATCH_EVENTS = 100


@app.post("/events", status_code=201)
async def create_event(request: Request) -> JSONResponse:
    """Ingest one event.

    Requires a valid ``X-Webhook-Signature: sha256=<hmac of raw body>``
    header computed with the shared secret from ``WEBHOOK_SECRET``.
    Replaying an existing idempotency key returns the stored event (200)
    and never enqueues a second delivery.

    Accepts an optional delivery window: ``deliver_after`` (RFC 3339,
    defaults to the creation moment) postpones the first delivery, and
    ``expires_at`` (RFC 3339, must be later than ``deliver_after``) bounds
    how long the event may wait in the queue. Invalid combinations are
    rejected with 422.
    """
    raw_body = await request.body()
    signature = request.headers.get(config.SIGNATURE_HEADER)
    if not _valid_signature(raw_body, signature):
        raise HTTPException(
            status_code=401, detail="invalid or missing X-Webhook-Signature"
        )

    try:
        data = json.loads(raw_body) if raw_body else None
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="request body must be JSON")
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="request body must be a JSON object")

    try:
        event_in = EventIn(**data)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors())

    try:
        event, created = db.create_event(
            idempotency_key=event_in.idempotency_key,
            target=str(event_in.target),
            payload=event_in.payload,
            deliver_after=event_in.deliver_after,
            expires_at=event_in.expires_at,
        )
    except db.InvalidDeliveryWindowError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    full = db.get_event(event["id"])
    response = JSONResponse(
        status_code=201 if created else 200, content=_public_event(full)
    )
    if not created:
        response.headers["Idempotent-Replay"] = "true"
    return response


@app.post("/events/batch")
async def create_events_batch(request: Request) -> JSONResponse:
    """Ingest a batch of 1–100 events atomically.

    The body is a JSON array of event objects identical to ``POST /events``
    and the signature is verified against the entire raw body, exactly as
    for single events.

    Semantics per item, in array order:

    - An idempotency key already stored returns the original event with
      ``created=false``; every other field of that item is ignored, valid
      or not, and nothing is re-enqueued.
    - A new key is validated like a single event (target URL, delivery
      window) and enqueued in array order.

    The batch is all-or-nothing for new events: a duplicate key *within*
    the batch, an invalid new event or any write failure means no new
    event of the batch is stored (422/500 and the database is untouched).
    Re-sending the same batch simply yields ``created=false`` for every
    item and never delivers anything twice. Responses are 201 when every
    item was newly created, otherwise 200 with ``Idempotent-Replay: true``.
    """
    raw_body = await request.body()
    signature = request.headers.get(config.SIGNATURE_HEADER)
    if not _valid_signature(raw_body, signature):
        raise HTTPException(
            status_code=401, detail="invalid or missing X-Webhook-Signature"
        )

    try:
        data = json.loads(raw_body) if raw_body else None
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="request body must be JSON")
    if not isinstance(data, list):
        raise HTTPException(status_code=400, detail="request body must be a JSON array")
    if not 1 <= len(data) <= MAX_BATCH_EVENTS:
        raise HTTPException(
            status_code=422,
            detail=f"batch must contain between 1 and {MAX_BATCH_EVENTS} events",
        )

    # Every item must at least carry a usable idempotency key — that is all
    # the stored-event lookup needs. Everything else is validated only for
    # items that turn out to be new.
    keys: list[str] = []
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            raise HTTPException(
                status_code=422, detail=f"item {i} must be a JSON object"
            )
        key = item.get("idempotency_key")
        if not isinstance(key, str) or not key:
            raise HTTPException(
                status_code=422,
                detail=f"item {i}: idempotency_key must be a non-empty string",
            )
        keys.append(key)
    seen: dict[str, int] = {}
    for i, key in enumerate(keys):
        if key in seen:
            raise HTTPException(
                status_code=422,
                detail=f"duplicate idempotency_key {key!r} in batch "
                f"(items {seen[key]} and {i})",
            )
        seen[key] = i

    existing = {e["idempotency_key"]: e for e in db.get_events_by_keys(keys)}

    plan: list[tuple[str, Any]] = []
    new_items: list[dict] = []
    for i, item in enumerate(data):
        stored = existing.get(item["idempotency_key"])
        if stored is not None:
            # Known key: the stored event wins and every other field of
            # this item is ignored, valid or not.
            plan.append(("replay", stored))
            continue
        try:
            event_in = EventIn(**item)
        except ValidationError as exc:
            raise HTTPException(
                status_code=422, detail={"index": i, "errors": exc.errors()}
            )
        plan.append(("new", len(new_items)))
        new_items.append(
            {
                "idempotency_key": event_in.idempotency_key,
                "target": str(event_in.target),
                "payload": event_in.payload,
                "deliver_after": event_in.deliver_after,
                "expires_at": event_in.expires_at,
            }
        )

    try:
        created_results = db.create_events_batch(new_items)
    except db.InvalidDeliveryWindowError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    events_out = []
    all_created = True
    for kind, ref in plan:
        if kind == "replay":
            event, created = ref, False
        else:
            event, created = created_results[ref]
        all_created = all_created and created
        full = db.get_event(event["id"])
        events_out.append({"created": created, "event": _public_event(full)})
    response = JSONResponse(
        status_code=201 if all_created else 200, content={"events": events_out}
    )
    if not all_created:
        response.headers["Idempotent-Replay"] = "true"
    return response


@app.get("/events")
async def list_events(
    status: str | None = Query(None, pattern="^(queued|processing|delivered|dead|expired)$"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    events = db.list_events(status=status, limit=limit, offset=offset)
    return {"events": [_public_event(e) for e in events], "limit": limit, "offset": offset}


@app.get("/events/key/{idempotency_key}")
async def get_event_by_key(idempotency_key: str):
    """Convenience lookup by idempotency key."""
    event = db.get_event_by_idempotency_key(idempotency_key)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return _public_event(event)


@app.get("/events/{event_id}")
async def get_event(event_id: str):
    """Return one event with every delivery attempt and its final status."""
    event = db.get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return _public_event(event)


@app.post("/events/{event_id}/replay")
async def replay_event(event_id: str):
    """Re-queue a dead-letter event for one fresh, full backoff round.

    Only ``dead`` events are eligible. The event is flipped back to
    ``queued`` in a single transaction, keeps its idempotency key and full
    attempt history (``attempt_no`` continues counting up), increments
    ``replay_count`` and is delivered again on the 1s/2s/4s schedule.
    Concurrent duplicate replays are applied at most once — the losers get
    409 and no extra delivery is enqueued.
    """
    try:
        event = db.replay_event(event_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="event not found")
    except db.ReplayNotAllowedError as exc:
        raise HTTPException(
            status_code=409,
            detail="only dead-letter events can be replayed "
            f"(current status: {exc.status})",
        )
    return _public_event(event)


@app.get("/targets/breakers")
async def get_target_breakers(
    state: str | None = Query(
        None, pattern="^(open|half-open|closed)$",
        description="Only return breakers in this state.",
    ),
    target: str | None = Query(None, description="Filter by exact target URL."),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Report the persistent per-target circuit breakers.

    Every target that has seen a delivery attempt appears once, with its
    current ``state`` (``closed`` / ``open`` / ``half-open``), the
    ``consecutive_failures`` counter that drives opening, the ``open_until``
    deadline of an open breaker (RFC 3339 UTC, ``null`` otherwise) and the
    ``probe_event_id`` of the single event probing a half-open breaker.
    State and deadlines survive restarts — they are read straight from
    SQLite.
    """
    breakers = db.list_breakers(state=state, target=target, limit=limit, offset=offset)
    return {"breakers": breakers, "limit": limit, "offset": offset}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/dev/sink", include_in_schema=False)
async def dev_sink(
    request: Request,
    fail_times: int = Query(0, description="fail the first N attempts per event id"),
    fail_status: int = Query(500),
):
    """Built-in target endpoint for local experiments.

    Fails the first ``fail_times`` delivery attempts of each event id, then
    answers 200. Point an event at
    ``http://localhost:8000/dev/sink?fail_times=4`` to watch it end up dead.
    """
    event_id = request.headers.get("x-event-id", "unknown")
    with _sink_lock:
        seen = _sink_calls.get(event_id, 0) + 1
        _sink_calls[event_id] = seen
    if seen <= fail_times:
        return Response(
            content=json.dumps({"ok": False, "attempt": seen}).encode(),
            status_code=fail_status,
            media_type="application/json",
        )
    return JSONResponse({"ok": True, "attempt": seen})

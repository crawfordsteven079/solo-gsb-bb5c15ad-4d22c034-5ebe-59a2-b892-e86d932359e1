import os
import tempfile

# Must be set before any app module is imported (config reads env at import).
_tmpdir = tempfile.mkdtemp(prefix="webhook-test-")
os.environ["WEBHOOK_SECRET"] = "test-secret"
os.environ["WEBHOOK_DB_PATH"] = os.path.join(_tmpdir, "test.db")
os.environ["WORKER_POLL_INTERVAL"] = "0.05"
os.environ["DELIVERY_TIMEOUT"] = "5"
# Short open window so the breaker's 1/2/4s backoff schedule (already
# larger than the window) dominates the existing retry-timing tests.
os.environ["BREAKER_OPEN_SECONDS"] = "1"

import socket
import threading
import time

import httpx
import pytest
import uvicorn

from app.main import app as main_app
from app import db


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ServerThread:
    def __init__(self, fastapi_app):
        self.port = _free_port()
        config = uvicorn.Config(
            fastapi_app, host="127.0.0.1", port=self.port, log_level="warning"
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self):
        self.thread.start()
        for _ in range(100):
            try:
                httpx.get(f"{self.base_url}/health", timeout=1)
                return self
            except httpx.HTTPError:
                time.sleep(0.05)
        raise RuntimeError("test server failed to start")

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(timeout=10)


@pytest.fixture(scope="session")
def server():
    db.init_db()
    _register_test_sinks(main_app)
    with ServerThread(main_app) as srv:
        yield srv


# --- test-only target endpoints mounted onto the real app under test ---

import asyncio  # noqa: E402
from fastapi import Request as FastAPIRequest  # noqa: E402
from fastapi.responses import JSONResponse as FastAPIJSONResponse  # noqa: E402

receipts: list[dict] = []
_fail_counts: dict[str, int] = {}
_gates: dict[str, threading.Event] = {}
_probe_counts: dict[str, int] = {}
_probe_gates: dict[str, threading.Event] = {}


def _register_test_sinks(application) -> None:
    import hashlib
    import hmac

    @application.post("/t/record")
    async def t_record(request: FastAPIRequest):
        body = await request.body()
        receipts.append(
            {
                "event_id": request.headers.get("x-event-id"),
                "idempotency_key": request.headers.get("x-idempotency-key"),
                "attempt": request.headers.get("x-delivery-attempt"),
                "signature": request.headers.get("x-webhook-signature"),
                "body": body.decode(),
            }
        )
        expected = (
            "sha256="
            + hmac.new(b"test-secret", body, hashlib.sha256).hexdigest()
        )
        assert request.headers.get("x-webhook-signature") == expected
        return FastAPIJSONResponse({"ok": True})

    @application.post("/t/block")
    async def t_block(request: FastAPIRequest, token: str = "default"):
        # Blocks until the matching gate is released, proving later events
        # for the same endpoint cannot overtake an earlier in-flight event.
        gate = _gates.setdefault(token, threading.Event())
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, gate.wait, 30)
        return FastAPIJSONResponse({"ok": True})

    @application.post("/t/release")
    async def t_release(token: str = "default"):
        _gates.setdefault(token, threading.Event()).set()
        return FastAPIJSONResponse({"released": True})

    @application.post("/t/fail-always")
    async def t_fail_always(request: FastAPIRequest):
        event_id = request.headers.get("x-event-id", "?")
        _fail_counts[event_id] = _fail_counts.get(event_id, 0) + 1
        # Slow only on the very first attempt so ordering can be observed.
        if _fail_counts[event_id] == 1:
            await asyncio.sleep(1.5)
        return FastAPIJSONResponse(
            {"ok": False, "n": _fail_counts[event_id]}, status_code=500
        )

    @application.post("/t/flaky")
    async def t_flaky(request: FastAPIRequest, fail_times: int = 0):
        # Records every delivery (like /t/record) but fails the first
        # ``fail_times`` attempts of each event, then answers 200 — ideal
        # for driving an event into the dead-letter queue before a replay.
        body = await request.body()
        event_id = request.headers.get("x-event-id", "?")
        receipts.append(
            {
                "event_id": event_id,
                "idempotency_key": request.headers.get("x-idempotency-key"),
                "attempt": request.headers.get("x-delivery-attempt"),
                "signature": request.headers.get("x-webhook-signature"),
                "body": body.decode(),
            }
        )
        _fail_counts[event_id] = _fail_counts.get(event_id, 0) + 1
        if _fail_counts[event_id] <= fail_times:
            return FastAPIJSONResponse({"ok": False}, status_code=500)
        return FastAPIJSONResponse({"ok": True})

    @application.post("/t/probe")
    async def t_probe(
        request: FastAPIRequest,
        fail_times: int = 0,
        gate: str | None = None,
    ):
        # Circuit-breaker driving sink, keyed per target URL (its query
        # string encodes fail_times/gate so parallel tests never collide).
        # Fails the first ``fail_times`` calls on this exact URL; the next
        # call blocks until the named gate is released, then answers 200 —
        # letting a test hold the single half-open probe in flight and
        # inspect the breaker state. Without a gate it simply keeps failing.
        url = str(request.url.path)
        if request.url.query:
            url = f"{url}?{request.url.query}"
        seen = _probe_counts.get(url, 0) + 1
        _probe_counts[url] = seen
        if seen <= fail_times:
            return FastAPIJSONResponse({"ok": False, "n": seen}, status_code=500)
        if gate is not None:
            event = _probe_gates.setdefault(gate, threading.Event())
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, event.wait, 30)
        else:
            return FastAPIJSONResponse({"ok": False, "n": seen}, status_code=500)
        return FastAPIJSONResponse({"ok": True, "n": seen})

    @application.post("/t/probe-release")
    async def t_probe_release(gate: str):
        _probe_gates.setdefault(gate, threading.Event()).set()
        return FastAPIJSONResponse({"released": True})



@pytest.fixture
def client():
    with httpx.Client(base_url="http://unused") as c:
        yield c


def wait_for(predicate, timeout=20.0, interval=0.1):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"condition not met; last={last!r}")

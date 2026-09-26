"""Background scheduler.

A single polling thread claims due head events (see ``db.claim_due_events``)
and hands each to a thread pool. The claim step guarantees strict ordering
per endpoint; the pool only parallelises *different* endpoints so one slow
target cannot stall everyone else.
"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from . import config, db
from .delivery import deliver

logger = logging.getLogger("webhook.worker")


class Worker:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pool = ThreadPoolExecutor(
            max_workers=config.WORKER_MAX_CONCURRENCY,
            thread_name_prefix="webhook-delivery",
        )

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="webhook-scheduler", daemon=True)
        self._thread.start()
        logger.info("scheduler started")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        self._pool.shutdown(wait=True, cancel_futures=True)
        logger.info("scheduler stopped")

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:  # the scheduler must never die
                logger.exception("scheduler tick failed")
            self._stop.wait(config.WORKER_POLL_INTERVAL)

    def tick(self) -> int:
        """Claim and dispatch all due events. Returns the number claimed."""
        claimed = db.claim_due_events()
        for event in claimed:
            self._pool.submit(self._handle, event)
        return len(claimed)

    @staticmethod
    def _handle(event: dict) -> None:
        logger.info(
            "delivering event %s to %s (attempt %d)",
            event["id"],
            event["target"],
            event["attempts_count"] + 1,
        )
        status_code, error = deliver(event)
        try:
            updated = db.record_attempt(event["id"], status_code, error)
        except Exception:
            logger.exception("failed to record attempt for event %s", event["id"])
            return
        if updated["status"] == "delivered":
            logger.info("event %s delivered (status=%s)", event["id"], status_code)
        elif updated["status"] == "dead":
            logger.warning(
                "event %s moved to dead-letter after %d attempts",
                event["id"],
                updated["attempts_count"],
            )
        else:
            logger.info(
                "event %s attempt failed (status=%s error=%s), retry scheduled",
                event["id"],
                status_code,
                error,
            )

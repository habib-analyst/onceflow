"""Webhook / event intake.

Producers either call :func:`ingest` directly or POST JSON to the tiny
HTTP handler. Intake preserves a producer-supplied idempotency key
verbatim — that key is the thread the DLQ replay later pulls on — and
only mints one when the producer did not supply it.
"""

from __future__ import annotations

import itertools
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from .ledger import AuditLog
from .models import Event

_counter = itertools.count(1)


def ingest(queue, raw: dict, audit: AuditLog | None = None) -> Event:
    """Validate an envelope minimally, normalise it, enqueue it."""
    if not isinstance(raw, dict):
        raise ValueError("event envelope must be a JSON object")
    n = next(_counter)
    event = Event(
        event_id=str(raw.get("event_id") or f"evt-{n}"),
        idempotency_key=str(raw.get("idempotency_key") or f"key-{n}"),
        aggregate_id=str(raw.get("aggregate_id") or "default"),
        seq=int(raw.get("seq") or 1),
        payload=raw.get("payload") or {},
    )
    queue.put(event)
    if audit:
        audit.log("intake", event_id=event.event_id, key=event.idempotency_key)
    return event


def make_handler(queue, audit: AuditLog | None = None):
    class WebhookHandler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802 - http.server API
            length = int(self.headers.get("Content-Length") or 0)
            try:
                raw = json.loads(self.rfile.read(length) or b"{}")
                event = ingest(queue, raw, audit)
            except Exception as exc:  # malformed envelope -> 400
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(exc)}).encode())
                return
            self.send_response(202)
            self.end_headers()
            self.wfile.write(
                json.dumps({"event_id": event.event_id,
                            "idempotency_key": event.idempotency_key}).encode()
            )

        def log_message(self, *args):  # keep tests and demos quiet
            pass

    return WebhookHandler


def serve(queue, audit: AuditLog | None = None, port: int = 0) -> HTTPServer:
    """Start a webhook HTTP server (port 0 = ephemeral). Caller shuts down."""
    server = HTTPServer(("127.0.0.1", port), make_handler(queue, audit))
    return server

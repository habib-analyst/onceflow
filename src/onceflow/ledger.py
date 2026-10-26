"""Effect ledger, completion store, and audit log.

The effect ledger is the ground truth about side effects: one entry per
*application* of an effect, keyed by idempotency key, and indexed by
event_id so duplicate applications of the same logical operation are
countable. The completion store records which keys finished cleanly.
The gap between "effect in ledger" and "key in completion store" is
exactly where replay duplicates are born, and where the forensic
classifier looks first.

Both stores can persist to an append-only JSONL file.
"""

from __future__ import annotations

import json
import os


class _JsonlSink:
    def __init__(self, path: str | None):
        self.path = path
        if path:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    def append(self, row: dict) -> None:
        if self.path:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, sort_keys=True) + "\n")


class EffectLedger:
    """Append-only record of applied effects."""

    def __init__(self, path: str | None = None):
        self._sink = _JsonlSink(path)
        self._by_key: dict[str, dict] = {}
        self._applications_by_event: dict[str, int] = {}

    def apply(self, event_id: str, key: str, effect: dict) -> None:
        entry = {"event_id": event_id, "key": key, "effect": effect}
        self._by_key.setdefault(key, entry)
        self._applications_by_event[event_id] = (
            self._applications_by_event.get(event_id, 0) + 1
        )
        self._sink.append({"kind": "effect", **entry})

    def has_effect_for_key(self, key: str) -> bool:
        return key in self._by_key

    def applications_for_event(self, event_id: str) -> int:
        return self._applications_by_event.get(event_id, 0)

    def duplicate_effects(self) -> int:
        """Extra applications beyond exactly-once, summed over events."""
        return sum(max(0, n - 1) for n in self._applications_by_event.values())

    def total_applications(self) -> int:
        return sum(self._applications_by_event.values())


class CompletionStore:
    """Keys whose workflow completed, plus event-level completion."""

    def __init__(self, path: str | None = None):
        self._sink = _JsonlSink(path)
        self._keys: set[str] = set()
        self._events: set[str] = set()

    def mark(self, event_id: str, key: str) -> None:
        self._keys.add(key)
        self._events.add(event_id)
        self._sink.append({"kind": "completion", "event_id": event_id, "key": key})

    def is_key_done(self, key: str) -> bool:
        return key in self._keys

    def is_event_done(self, event_id: str) -> bool:
        return event_id in self._events


class AuditLog:
    """Append-only audit trail of every pipeline decision."""

    def __init__(self, path: str | None = None):
        self._sink = _JsonlSink(path)
        self.rows: list[dict] = []
        self._step = 0

    def log(self, kind: str, **fields) -> None:
        self._step += 1
        row = {"step": self._step, "kind": kind, **fields}
        self.rows.append(row)
        self._sink.append(row)

    def count(self, kind: str) -> int:
        return sum(1 for r in self.rows if r["kind"] == kind)

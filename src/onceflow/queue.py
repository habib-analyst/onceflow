"""Event queues: in-process and file-backed (append-only JSONL log)."""

from __future__ import annotations

import json
import os
from collections import deque

from .models import Event


def event_to_row(event: Event) -> dict:
    return {
        "event_id": event.event_id,
        "idempotency_key": event.idempotency_key,
        "aggregate_id": event.aggregate_id,
        "seq": event.seq,
        "payload": event.payload,
    }


def event_from_row(row: dict) -> Event:
    return Event(
        event_id=row["event_id"],
        idempotency_key=row["idempotency_key"],
        aggregate_id=row["aggregate_id"],
        seq=row["seq"],
        payload=row["payload"],
    )


class InMemoryQueue:
    def __init__(self):
        self._q: deque[Event] = deque()

    def put(self, event: Event) -> None:
        self._q.append(event)

    def get(self) -> Event | None:
        return self._q.popleft() if self._q else None

    def __len__(self) -> int:
        return len(self._q)


class FileQueue:
    """Durable queue backed by an append-only JSONL log with a read offset.

    Writes survive process death; a consumer re-opening the file resumes
    from a persisted offset file, so a crash mid-drain does not lose or
    re-deliver entries it already consumed from the log's perspective.
    """

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._offset_path = path + ".offset"
        self._offset = 0
        if os.path.exists(self._offset_path):
            with open(self._offset_path, encoding="utf-8") as fh:
                self._offset = int(fh.read().strip() or 0)

    def put(self, event: Event) -> None:
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(event_to_row(event), sort_keys=True) + "\n")

    def _rows(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        with open(self.path, encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]

    def get(self) -> Event | None:
        rows = self._rows()
        if self._offset >= len(rows):
            return None
        event = event_from_row(rows[self._offset])
        self._offset += 1
        with open(self._offset_path, "w", encoding="utf-8") as fh:
            fh.write(str(self._offset))
        return event

    def __len__(self) -> int:
        return max(0, len(self._rows()) - self._offset)

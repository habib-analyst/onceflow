"""Core data types: events, failure records, classification verdicts."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Event:
    """One unit of work travelling through the pipeline.

    ``idempotency_key`` is producer-assigned and stable for the logical
    operation. It must survive DLQ replay unchanged; regenerating it is
    what makes naive replay double-apply effects.
    """

    event_id: str
    idempotency_key: str
    aggregate_id: str
    seq: int
    payload: dict


class FailureClass(enum.Enum):
    TRANSIENT = "transient"
    POISON = "poison"
    DEPENDENCY_DOWN = "dependency_down"
    SCOPE_ERROR = "scope_error"
    ALREADY_APPLIED = "already_applied"
    UNKNOWN = "unknown"


class ReplayAction(enum.Enum):
    REPLAY = "replay"                # safe to re-execute with original key
    RESOLVE = "resolve"              # effect already landed; just complete it
    QUARANTINE = "quarantine"        # never auto-replay; human/fix required
    DEFER = "defer"                  # replayable later (dependency still down)


@dataclass
class FailureRecord:
    """Everything the forensic classifier is allowed to look at."""

    event: Event
    attempts: int
    error_type: str
    error_message: str
    phase: str  # validation | scope | ordering | dependency | transient | post_effect_crash
    first_failed_step: int
    last_failed_step: int


@dataclass
class Classification:
    failure_class: FailureClass
    action: ReplayAction
    reason: str


@dataclass
class DLQEntry:
    record: FailureRecord
    classification: Classification | None = None
    history: list[str] = field(default_factory=list)

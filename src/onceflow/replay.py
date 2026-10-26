"""Live processing and the three DLQ recovery strategies under test.

The live phase is identical for every strategy: a standard executor,
up to ``max_attempts`` in-place attempts per delivery, then the failure
is recorded and the event dead-letters. A crash after the effect goes
straight to the DLQ — its outcome is unknown, which is the point.

The strategies differ only in what they do with the DLQ afterwards:

* ``no_replay``       — the DLQ is terminal. Nothing is re-delivered.
* ``naive_replay``    — the classic script: re-publish every DLQ payload
  as a *new* message (fresh idempotency key), in DLQ order, for several
  sweeps, with no classification. Poison and scope errors are
  re-delivered every sweep; crash victims re-apply their effects.
* ``classified_replay`` (onceflow) — forensic classification first.
  Already-applied effects are resolved without re-execution; poison and
  scope errors are quarantined; only transient/dependency classes are
  replayed, with the *original* idempotency key preserved, through the
  reconciling executor, in per-aggregate sequence order.
"""

from __future__ import annotations

from .classifier import classify
from .errors import CrashAfterEffectError, OnceflowError
from .executor import ReconcilingExecutor, Runtime, StandardExecutor
from .models import DLQEntry, Event, FailureRecord, ReplayAction

_PHASE_BY_ERROR = {
    "PayloadValidationError": "validation",
    "ScopeDeniedError": "scope",
    "OrderingViolationError": "ordering",
    "DependencyDownError": "dependency",
    "TransientProcessingError": "transient",
    "CrashAfterEffectError": "post_effect_crash",
}


def _record(rt: Runtime, event: Event, exc: OnceflowError, step: int) -> FailureRecord:
    return FailureRecord(
        event=event,
        attempts=rt.attempts.get(event.event_id, 1),
        error_type=type(exc).__name__,
        error_message=str(exc),
        phase=_PHASE_BY_ERROR.get(type(exc).__name__, "unknown"),
        first_failed_step=step,
        last_failed_step=step,
    )


def process_stream(
    rt: Runtime, stream: list[Event], max_attempts: int = 3
) -> list[DLQEntry]:
    """Run the live phase. Returns the DLQ entries it produced."""
    executor = StandardExecutor(rt)
    dlq: list[DLQEntry] = []
    for step, event in enumerate(stream):
        if rt.completion.is_key_done(event.idempotency_key):
            rt.audit.log("dedup_skip", event_id=event.event_id,
                         key=event.idempotency_key)
            continue
        for attempt in range(max_attempts):
            try:
                executor.execute(event)
                rt.audit.log("applied", event_id=event.event_id,
                             key=event.idempotency_key)
                break
            except CrashAfterEffectError as exc:
                # Outcome unknown; do not retry in place. Forensics decide.
                dlq.append(DLQEntry(_record(rt, event, exc, step)))
                rt.audit.log("dlq", event_id=event.event_id,
                             reason="crash_after_effect")
                break
            except OnceflowError as exc:
                if attempt == max_attempts - 1:
                    dlq.append(DLQEntry(_record(rt, event, exc, step)))
                    rt.audit.log("dlq", event_id=event.event_id,
                                 reason=type(exc).__name__)
    return dlq


def no_replay(rt: Runtime, dlq: list[DLQEntry]) -> dict:
    """Baseline (b): the DLQ is terminal."""
    return {"deliveries": [], "quarantined": 0, "resolved": 0,
            "remaining_dlq": len(dlq)}


def naive_replay(rt: Runtime, dlq: list[DLQEntry], sweeps: int = 3) -> dict:
    """Baseline (a): re-publish everything, fresh keys, no classification."""
    executor = StandardExecutor(rt)
    deliveries: list[str] = []
    remaining = list(dlq)
    counter = 0
    for sweep in range(sweeps):
        still_failing: list[DLQEntry] = []
        for entry in remaining:
            counter += 1
            src = entry.record.event
            republished = Event(
                event_id=src.event_id,
                idempotency_key=f"{src.idempotency_key}::naive{sweep}:{counter}",
                aggregate_id=src.aggregate_id,
                seq=src.seq,
                payload=src.payload,
            )
            deliveries.append(src.event_id)
            try:
                executor.execute(republished)
                rt.audit.log("naive_replay_applied", event_id=src.event_id,
                             sweep=sweep)
            except OnceflowError:
                still_failing.append(entry)
        remaining = still_failing
        if not remaining:
            break
    return {"deliveries": deliveries, "quarantined": 0, "resolved": 0,
            "remaining_dlq": len(remaining)}


def classified_replay(rt: Runtime, dlq: list[DLQEntry]) -> dict:
    """onceflow (c): classify first, replay only what is safe, keys intact."""
    executor = ReconcilingExecutor(rt)
    deliveries: list[str] = []
    quarantined = 0
    resolved = 0
    to_replay: list[DLQEntry] = []
    remaining: list[DLQEntry] = []

    for entry in dlq:
        verdict = classify(
            entry.record,
            rt.ledger,
            rt.scopes,
            rt.dependency_healthy,
        )
        entry.classification = verdict
        entry.history.append(f"classified:{verdict.failure_class.value}")
        rt.audit.log("classify", event_id=entry.record.event.event_id,
                     failure_class=verdict.failure_class.value,
                     action=verdict.action.value)
        if verdict.action is ReplayAction.RESOLVE:
            event = entry.record.event
            rt.completion.mark(event.event_id, event.idempotency_key)
            expected = rt.expected_seq.get(event.aggregate_id, 1)
            if expected == event.seq:
                rt.expected_seq[event.aggregate_id] = event.seq + 1
            resolved += 1
            rt.audit.log("resolved_without_replay",
                         event_id=event.event_id, key=event.idempotency_key)
        elif verdict.action is ReplayAction.QUARANTINE:
            quarantined += 1
        elif verdict.action is ReplayAction.DEFER:
            remaining.append(entry)
        else:  # REPLAY
            to_replay.append(entry)

    # Replay in per-aggregate sequence order so ordering failures heal.
    to_replay.sort(key=lambda e: (e.record.event.aggregate_id,
                                  e.record.event.seq))
    for entry in to_replay:
        event = entry.record.event
        deliveries.append(event.event_id)
        try:
            executor.execute(event)
            rt.audit.log("replay_applied", event_id=event.event_id,
                         key=event.idempotency_key)
        except OnceflowError as exc:
            entry.history.append(f"replay_failed:{type(exc).__name__}")
            remaining.append(entry)

    return {"deliveries": deliveries, "quarantined": quarantined,
            "resolved": resolved, "remaining_dlq": len(remaining)}

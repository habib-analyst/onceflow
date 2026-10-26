"""Dead-letter forensics: classify WHY an execution failed.

The classifier reads only the failure record plus three live probes:

* does the effect ledger already hold an effect under the event's
  original idempotency key?  (the crash-after-effect case)
* is the payload still invalid against the schema?  (poison never heals)
* is the referenced scope authorised now?           (scope never heals
  by replaying; it needs a grant or a corrected producer)
* is the downstream dependency healthy now?         (dependency failures
  are replayable only after recovery)

Rules are ordered: an effect that already landed is resolved before
anything is re-executed, and anything the classifier cannot explain is
quarantined, never replayed. The safe default is *don't touch it*.
"""

from __future__ import annotations

from .errors import PayloadValidationError
from .executor import validate_payload
from .ledger import EffectLedger
from .models import (
    Classification,
    FailureClass,
    FailureRecord,
    ReplayAction,
)


def classify(
    record: FailureRecord,
    ledger: EffectLedger,
    scopes: set[str],
    dependency_healthy: bool,
) -> Classification:
    event = record.event
    key = event.idempotency_key

    # 1. The forensic question first: did the effect actually land?
    if ledger.has_effect_for_key(key):
        return Classification(
            FailureClass.ALREADY_APPLIED,
            ReplayAction.RESOLVE,
            "effect ledger holds an effect under the original idempotency "
            "key; the failure happened after the effect (e.g. crash before "
            "the completion record). Resolve without re-executing.",
        )

    # 2. Poison: the payload itself can never succeed.
    try:
        validate_payload(event.payload)
    except PayloadValidationError as exc:
        return Classification(
            FailureClass.POISON,
            ReplayAction.QUARANTINE,
            f"payload fails schema validation ({exc}); replaying it any "
            "number of times fails the same way. Quarantine for a human "
            "or a producer fix.",
        )

    # 3. Scope: an authorisation fact, not a timing fact.
    if event.payload.get("scope") not in scopes:
        return Classification(
            FailureClass.SCOPE_ERROR,
            ReplayAction.QUARANTINE,
            "event references a scope the consumer is not authorised "
            "for; replay cannot change an authorisation fact. Quarantine "
            "until a grant or a corrected event exists.",
        )

    # 4. Dependency: replayable, but only once the dependency is back.
    if record.error_type == "DependencyDownError" or (
        event.payload.get("requires_dependency") and not dependency_healthy
        and record.error_type in ("DependencyDownError", "TransientProcessingError")
    ):
        if dependency_healthy:
            return Classification(
                FailureClass.DEPENDENCY_DOWN,
                ReplayAction.REPLAY,
                "failed because the downstream dependency was down; the "
                "dependency probe is healthy now, so replay is safe.",
            )
        return Classification(
            FailureClass.DEPENDENCY_DOWN,
            ReplayAction.DEFER,
            "dependency still down; hold in the DLQ instead of burning "
            "re-deliveries against a dead dependency.",
        )

    # 5. Timing/transient classes: safe to replay with the original key.
    if record.error_type in (
        "OrderingViolationError",
        "TransientProcessingError",
        "CrashAfterEffectError",
    ):
        return Classification(
            FailureClass.TRANSIENT,
            ReplayAction.REPLAY,
            f"{record.error_type}: a timing failure, and the effect "
            "ledger shows no application under the original key, so "
            "re-execution cannot double-apply.",
        )

    # 6. Unexplained: quarantine. The classifier never guesses a replay.
    return Classification(
        FailureClass.UNKNOWN,
        ReplayAction.QUARANTINE,
        f"unrecognised failure ({record.error_type}: "
        f"{record.error_message}); quarantined rather than replayed on "
        "a guess.",
    )

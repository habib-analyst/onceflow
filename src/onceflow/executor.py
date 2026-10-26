"""The workflow handler and its executors.

The sample workflow applies one effect per event: record a payment-like
effect for ``payload.account``. Handler steps, in order:

1. validate the payload against the schema        -> PayloadValidationError
2. check the payload's scope is authorised        -> ScopeDeniedError
3. check per-aggregate sequence continuity        -> OrderingViolationError
4. call the downstream dependency, if required    -> DependencyDownError
5. suffer an injected transient fault, if armed   -> TransientProcessingError
6. apply the effect to the effect ledger
7. suffer an injected crash, if armed              -> CrashAfterEffectError
8. write the completion record

Faults are armed per event_id by the scenario (see bench.py), never by
the payload, so the handler logic itself stays production-shaped.

Two executors share this handler:

* ``StandardExecutor`` — the common pattern: dedupe on the completion
  record only. If a worker crashes between steps 6 and 8, a later
  delivery re-applies the effect. This is the trap.
* ``ReconcilingExecutor`` — onceflow's executor: before step 6 it checks
  the effect ledger for the event's *original* idempotency key. If the
  effect already landed, it skips re-application and only writes the
  completion record. This is what makes classified replay safe.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import (
    CrashAfterEffectError,
    DependencyDownError,
    OrderingViolationError,
    PayloadValidationError,
    ScopeDeniedError,
    TransientProcessingError,
)
from .ledger import AuditLog, CompletionStore, EffectLedger
from .models import Event


@dataclass
class FaultPlan:
    """Faults the injector arms for one event_id."""

    transient_failures: int = 0          # fail this many attempts transiently
    crash_after_effect: bool = False     # crash once, after the effect lands


@dataclass
class Runtime:
    """Mutable world state shared by handler, classifier, and strategies."""

    ledger: EffectLedger
    completion: CompletionStore
    audit: AuditLog
    scopes: set[str] = field(default_factory=set)
    dependency_healthy: bool = True
    expected_seq: dict[str, int] = field(default_factory=dict)
    faults: dict[str, FaultPlan] = field(default_factory=dict)
    attempts: dict[str, int] = field(default_factory=dict)
    handler_invocations: int = 0

    def fault_for(self, event_id: str) -> FaultPlan:
        return self.faults.setdefault(event_id, FaultPlan())


def validate_payload(payload: dict) -> None:
    if not isinstance(payload, dict):
        raise PayloadValidationError("payload is not an object")
    account = payload.get("account")
    amount = payload.get("amount")
    currency = payload.get("currency")
    scope = payload.get("scope")
    if not isinstance(account, str) or not account:
        raise PayloadValidationError("account missing or not a string")
    if not isinstance(amount, (int, float)) or isinstance(amount, bool) or amount <= 0:
        raise PayloadValidationError("amount missing or not a positive number")
    if not isinstance(currency, str) or len(currency) != 3:
        raise PayloadValidationError("currency missing or not a 3-letter code")
    if not isinstance(scope, str) or not scope:
        raise PayloadValidationError("scope missing or not a string")


class _BaseExecutor:
    reconcile_effect_ledger = False

    def __init__(self, runtime: Runtime):
        self.rt = runtime

    def execute(self, event: Event) -> str:
        """Run the workflow once. Returns 'applied' or 'already_done'.

        Raises a typed OnceflowError on failure; CrashAfterEffectError
        means the effect landed but completion was never recorded.
        """
        rt = self.rt
        rt.handler_invocations += 1
        rt.attempts[event.event_id] = rt.attempts.get(event.event_id, 0) + 1
        attempt_no = rt.attempts[event.event_id]

        if rt.completion.is_key_done(event.idempotency_key):
            return "already_done"

        payload = event.payload
        validate_payload(payload)

        if payload["scope"] not in rt.scopes:
            raise ScopeDeniedError(f"scope {payload['scope']!r} not authorised")

        expected = rt.expected_seq.get(event.aggregate_id, 1)
        if event.seq != expected:
            raise OrderingViolationError(
                f"aggregate {event.aggregate_id}: expected seq {expected}, got {event.seq}"
            )

        if payload.get("requires_dependency") and not rt.dependency_healthy:
            raise DependencyDownError("downstream dependency unavailable")

        plan = rt.fault_for(event.event_id)
        if plan.transient_failures > 0:
            plan.transient_failures -= 1
            raise TransientProcessingError("injected transient failure")

        if self.reconcile_effect_ledger and rt.ledger.has_effect_for_key(
            event.idempotency_key
        ):
            # The effect already landed (e.g. crash after effect). Do not
            # re-apply; just write the completion record under the
            # original key and advance the sequence.
            rt.completion.mark(event.event_id, event.idempotency_key)
            rt.expected_seq[event.aggregate_id] = expected + 1
            rt.audit.log(
                "reconciled", event_id=event.event_id, key=event.idempotency_key
            )
            return "already_done"

        rt.ledger.apply(
            event.event_id,
            event.idempotency_key,
            {"account": payload["account"], "amount": payload["amount"],
             "currency": payload["currency"]},
        )

        if plan.crash_after_effect and attempt_no == 1:
            raise CrashAfterEffectError(
                "worker crashed after effect, before completion record"
            )

        rt.completion.mark(event.event_id, event.idempotency_key)
        rt.expected_seq[event.aggregate_id] = expected + 1
        return "applied"


class StandardExecutor(_BaseExecutor):
    """Completion-record dedupe only. The common, unsafe-under-crash pattern."""

    reconcile_effect_ledger = False


class ReconcilingExecutor(_BaseExecutor):
    """onceflow's executor: reconciles against the effect ledger first."""

    reconcile_effect_ledger = True

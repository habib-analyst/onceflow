import pytest

from onceflow.classifier import classify
from onceflow.errors import PayloadValidationError
from onceflow.ledger import EffectLedger
from onceflow.models import FailureClass, FailureRecord, Event, ReplayAction

SCOPES = {"payments.write"}


def ev(payload=None, key="k1", seq=1, agg="a1"):
    base = {"account": "acct-1", "amount": 25, "currency": "USD",
            "scope": "payments.write"}
    if payload:
        base.update(payload)
    return Event("e1", key, agg, seq, base)


def record(event, error_type, phase="transient"):
    return FailureRecord(event, attempts=3, error_type=error_type,
                         error_message="boom", phase=phase,
                         first_failed_step=0, last_failed_step=0)


def test_crash_with_effect_in_ledger_resolves_without_replay():
    ledger = EffectLedger()
    ledger.apply("e1", "k1", {"account": "acct-1", "amount": 25})
    verdict = classify(record(ev(), "CrashAfterEffectError",
                              "post_effect_crash"),
                       ledger, SCOPES, dependency_healthy=True)
    assert verdict.failure_class is FailureClass.ALREADY_APPLIED
    assert verdict.action is ReplayAction.RESOLVE


def test_poison_payload_is_quarantined():
    bad = ev({"amount": "ten"})
    verdict = classify(record(bad, "PayloadValidationError", "validation"),
                       EffectLedger(), SCOPES, dependency_healthy=True)
    assert verdict.failure_class is FailureClass.POISON
    assert verdict.action is ReplayAction.QUARANTINE


def test_scope_error_is_quarantined():
    scoped = ev({"scope": "payments.admin"})
    verdict = classify(record(scoped, "ScopeDeniedError", "scope"),
                       EffectLedger(), SCOPES, dependency_healthy=True)
    assert verdict.failure_class is FailureClass.SCOPE_ERROR
    assert verdict.action is ReplayAction.QUARANTINE


def test_dependency_down_replays_once_healthy():
    dep = ev({"requires_dependency": True})
    verdict = classify(record(dep, "DependencyDownError", "dependency"),
                       EffectLedger(), SCOPES, dependency_healthy=True)
    assert verdict.failure_class is FailureClass.DEPENDENCY_DOWN
    assert verdict.action is ReplayAction.REPLAY


def test_dependency_down_defers_while_still_down():
    dep = ev({"requires_dependency": True})
    verdict = classify(record(dep, "DependencyDownError", "dependency"),
                       EffectLedger(), SCOPES, dependency_healthy=False)
    assert verdict.action is ReplayAction.DEFER


def test_transient_replays():
    verdict = classify(record(ev(), "TransientProcessingError"),
                       EffectLedger(), SCOPES, dependency_healthy=True)
    assert verdict.failure_class is FailureClass.TRANSIENT
    assert verdict.action is ReplayAction.REPLAY


def test_crash_without_effect_in_ledger_is_transient():
    # Crash happened but no effect landed under this key: re-execution
    # cannot double-apply, so a replay is safe.
    verdict = classify(record(ev(), "CrashAfterEffectError",
                              "post_effect_crash"),
                       EffectLedger(), SCOPES, dependency_healthy=True)
    assert verdict.failure_class is FailureClass.TRANSIENT
    assert verdict.action is ReplayAction.REPLAY


def test_unknown_failure_is_quarantined_not_guessed():
    verdict = classify(record(ev(), "SomeNewError"),
                       EffectLedger(), SCOPES, dependency_healthy=True)
    assert verdict.failure_class is FailureClass.UNKNOWN
    assert verdict.action is ReplayAction.QUARANTINE

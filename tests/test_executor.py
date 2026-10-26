import pytest

from onceflow.errors import CrashAfterEffectError
from onceflow.executor import (
    FaultPlan,
    ReconcilingExecutor,
    Runtime,
    StandardExecutor,
)
from onceflow.ledger import AuditLog, CompletionStore, EffectLedger
from onceflow.models import Event


def runtime(faults=None):
    return Runtime(
        ledger=EffectLedger(),
        completion=CompletionStore(),
        audit=AuditLog(),
        scopes={"payments.write"},
        dependency_healthy=True,
        faults=faults or {},
    )


def ev(event_id="e1", key="k1", agg="a1", seq=1, **payload_over):
    payload = {"account": "acct-1", "amount": 25, "currency": "USD",
               "scope": "payments.write"}
    payload.update(payload_over)
    return Event(event_id, key, agg, seq, payload)


def test_same_key_twice_applies_effect_once():
    rt = runtime()
    ex = StandardExecutor(rt)
    assert ex.execute(ev()) == "applied"
    assert ex.execute(ev()) == "already_done"
    assert rt.ledger.applications_for_event("e1") == 1
    assert rt.ledger.duplicate_effects() == 0


def test_standard_executor_double_applies_after_crash():
    # The trap, documented as a test: crash between effect and
    # completion record, then redeliver -> the standard executor
    # applies the effect a second time.
    rt = runtime({"e1": FaultPlan(crash_after_effect=True)})
    ex = StandardExecutor(rt)
    with pytest.raises(CrashAfterEffectError):
        ex.execute(ev())
    assert ex.execute(ev()) == "applied"
    assert rt.ledger.applications_for_event("e1") == 2
    assert rt.ledger.duplicate_effects() == 1


def test_reconciling_executor_never_double_applies_after_crash():
    rt = runtime({"e1": FaultPlan(crash_after_effect=True)})
    ex = ReconcilingExecutor(rt)
    with pytest.raises(CrashAfterEffectError):
        ex.execute(ev())
    assert ex.execute(ev()) == "already_done"
    assert rt.ledger.applications_for_event("e1") == 1
    assert rt.ledger.duplicate_effects() == 0
    assert rt.completion.is_event_done("e1")


def test_new_key_defeats_completion_dedupe():
    # Why replay must preserve the key: the same logical operation
    # under a regenerated key is invisible to the completion store.
    rt = runtime()
    ex = StandardExecutor(rt)
    assert ex.execute(ev(key="k1")) == "applied"
    assert ex.execute(ev(key="k1::republished", agg="a2")) == "applied"
    assert rt.ledger.duplicate_effects() == 1

from onceflow.executor import FaultPlan, Runtime
from onceflow.ledger import AuditLog, CompletionStore, EffectLedger
from onceflow.models import Event
from onceflow.replay import classified_replay, naive_replay, process_stream

SCOPES = {"payments.write"}


def runtime(dependency_healthy=False, faults=None):
    return Runtime(
        ledger=EffectLedger(),
        completion=CompletionStore(),
        audit=AuditLog(),
        scopes=set(SCOPES),
        dependency_healthy=dependency_healthy,
        faults=faults or {},
    )


def ev(event_id, kind_payload=None, agg=None, seq=1):
    payload = {"account": "acct-1", "amount": 25, "currency": "USD",
               "scope": "payments.write"}
    if kind_payload:
        payload.update(kind_payload)
    return Event(event_id, f"key-{event_id}", agg or f"agg-{event_id}",
                 seq, payload)


def build_rt():
    """One of each interesting failure, through the real live phase."""
    faults = {
        "crash": FaultPlan(crash_after_effect=True),
        "flaky": FaultPlan(transient_failures=3),
    }
    rt = runtime(faults=faults)
    stream = [
        ev("ok"),
        ev("crash"),
        ev("flaky"),
        ev("dep", {"requires_dependency": True}),
        ev("poison", {"amount": "ten"}),
        ev("scope", {"scope": "payments.admin"}),
    ]
    dlq = process_stream(rt, stream, max_attempts=3)
    rt.dependency_healthy = True  # dependency recovers
    return rt, dlq


def test_live_phase_dead_letters_everything_except_ok():
    rt, dlq = build_rt()
    assert rt.completion.is_event_done("ok")
    assert {e.record.event.event_id for e in dlq} == {
        "crash", "flaky", "dep", "poison", "scope"}


def test_classified_replay_recovers_all_recoverable_with_zero_duplicates():
    rt, dlq = build_rt()
    outcome = classified_replay(rt, dlq)
    for event_id in ("crash", "flaky", "dep"):
        assert rt.completion.is_event_done(event_id)
    assert rt.ledger.duplicate_effects() == 0
    assert outcome["resolved"] == 1          # the crash victim
    assert outcome["quarantined"] == 2       # poison + scope
    assert outcome["remaining_dlq"] == 0
    # Poison and scope were never re-delivered to the handler.
    assert "poison" not in outcome["deliveries"]
    assert "scope" not in outcome["deliveries"]


def test_naive_replay_recovers_but_double_applies_the_crash_victim():
    rt, dlq = build_rt()
    outcome = naive_replay(rt, dlq, sweeps=3)
    for event_id in ("crash", "flaky", "dep"):
        assert rt.completion.is_event_done(event_id)
    assert rt.ledger.duplicate_effects() == 1  # the crash victim, twice
    # Poison was re-delivered on every sweep.
    assert outcome["deliveries"].count("poison") == 3
    assert outcome["deliveries"].count("scope") == 3


def test_no_replay_leaves_recoverable_work_dead():
    from onceflow.replay import no_replay
    rt, dlq = build_rt()
    no_replay(rt, dlq)
    for event_id in ("crash", "flaky", "dep"):
        assert not rt.completion.is_event_done(event_id)
    assert rt.ledger.duplicate_effects() == 0

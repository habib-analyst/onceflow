"""Fault-injection benchmark: naive replay vs no replay vs onceflow.

Scenario (deterministic per seed): a stream of payment-like events is
delivered to the live pipeline while a downstream dependency is down.
The stream mixes, in fixed proportions:

* normal events                     (30%)
* duplicate deliveries              (10%)  delivered twice, back to back
* dependency events                 (15%)  need the dependency; it is
                                           down for the whole live phase
                                           and recovers before replay
* crash-after-effect events         (10%)  effect lands, worker dies
                                           before the completion record
* out-of-order events               (10%)  per-aggregate chains of 3,
                                           delivered scrambled
* transient-failure events          (10%)  fail all live attempts,
                                           succeed once replayed
* poison payloads                   (10%)  never validate, ever
* scope-error events                 (5%)  reference an unauthorised
                                           scope; replay cannot fix that

Every strategy runs the identical live phase on an identical fresh copy
of the scenario, then recovers the DLQ its own way. Metrics:

* duplicate_effects / duplicate_effect_rate - effect applications
  beyond exactly-once, per intended (recoverable) effect. The number
  that matters: each one is a customer charged twice.
* recovery_rate - recoverable events that reached completion.
* poison_redeliveries - times a poison payload was delivered to the
  handler during the recovery phase.
* handler_invocations - total handler runs (cost of recovery).

Run:  python -m onceflow.bench --seeds 7 19 42 --n 600 --out results.json
"""

from __future__ import annotations

import argparse
import json
import random

from .executor import FaultPlan, Runtime
from .ledger import AuditLog, CompletionStore, EffectLedger
from .models import Event
from .replay import classified_replay, naive_replay, no_replay, process_stream

AUTHORISED_SCOPES = {"payments.write"}

# kind -> share of n. Ordering is emitted in chains of 3, so its count
# is rounded down to a multiple of 3.
_MIX = [
    ("poison", 0.10),
    ("scope", 0.05),
    ("dependency", 0.15),
    ("crash", 0.10),
    ("ordering", 0.10),
    ("transient", 0.10),
    ("duplicate", 0.10),
    ("normal", 0.30),
]


class Scenario:
    def __init__(self, seed: int, n: int):
        self.seed = seed
        self.n = n
        self.stream: list[Event] = []
        self.faults: dict[str, FaultPlan] = {}
        self.poison_ids: set[str] = set()
        self.recoverable_ids: set[str] = set()
        self._build()

    def _payload(self, i: int, kind: str) -> dict:
        payload = {
            "account": f"acct-{i % 50}",
            "amount": 10 + (i % 90),
            "currency": "USD",
            "scope": "payments.write",
        }
        if kind == "poison":
            # Alternating poison shapes: wrong type, or a missing field.
            if i % 2 == 0:
                payload["amount"] = "ten"
            else:
                del payload["currency"]
        elif kind == "scope":
            payload["scope"] = "payments.admin"  # not authorised
        elif kind == "dependency":
            payload["requires_dependency"] = True
        return payload

    def _build(self) -> None:
        rng = random.Random(self.seed)
        kinds: list[str] = []
        assigned = 0
        for kind, share in _MIX:
            count = int(self.n * share)
            if kind == "ordering":
                count -= count % 3
            kinds.extend([kind] * count)
            assigned += count
        kinds.extend(["normal"] * (self.n - assigned))
        rng.shuffle(kinds)

        events: list[Event] = []
        ordering_groups: dict[str, list[Event]] = {}
        group_no = 0
        pending_ordering: list[Event] = []
        for i, kind in enumerate(kinds):
            event_id = f"evt-{self.seed}-{i}"
            if kind == "ordering":
                if not pending_ordering:
                    group_no += 1
                aggregate = f"ord-{self.seed}-{group_no}"
                seq = len(pending_ordering) + 1
                event = Event(event_id, f"key-{self.seed}-{i}", aggregate,
                              seq, self._payload(i, kind))
                pending_ordering.append(event)
                if len(pending_ordering) == 3:
                    ordering_groups[aggregate] = pending_ordering
                    pending_ordering = []
            else:
                event = Event(event_id, f"key-{self.seed}-{i}",
                              f"agg-{self.seed}-{i}", 1,
                              self._payload(i, kind))
            events.append(event)
            if kind == "poison":
                self.poison_ids.add(event_id)
            else:
                if kind != "scope":
                    self.recoverable_ids.add(event_id)
            if kind == "transient":
                self.faults[event_id] = FaultPlan(transient_failures=3)
            elif kind == "crash":
                self.faults[event_id] = FaultPlan(crash_after_effect=True)

        # Build the delivery stream. Ordering groups are emitted whole,
        # scrambled as (seq2, seq3, seq1); duplicates are delivered twice.
        kind_by_id = {f"evt-{self.seed}-{i}": k for i, k in enumerate(kinds)}
        emitted_groups: set[str] = set()
        for event in events:
            kind = kind_by_id[event.event_id]
            if kind == "ordering":
                if event.aggregate_id in emitted_groups:
                    continue
                emitted_groups.add(event.aggregate_id)
                group = ordering_groups[event.aggregate_id]
                self.stream.extend([group[1], group[2], group[0]])
            elif kind == "duplicate":
                self.stream.extend([event, event])
            else:
                self.stream.append(event)


def run_strategy(name: str, scenario: Scenario) -> dict:
    rt = Runtime(
        ledger=EffectLedger(),
        completion=CompletionStore(),
        audit=AuditLog(),
        scopes=set(AUTHORISED_SCOPES),
        dependency_healthy=False,  # the incident: dependency down live
        faults={eid: FaultPlan(p.transient_failures, p.crash_after_effect)
                for eid, p in scenario.faults.items()},
    )
    dlq = process_stream(rt, scenario.stream, max_attempts=3)
    live_invocations = rt.handler_invocations

    rt.dependency_healthy = True  # dependency recovers before recovery
    if name == "naive_replay":
        outcome = naive_replay(rt, dlq, sweeps=3)
    elif name == "no_replay":
        outcome = no_replay(rt, dlq)
    elif name == "classified_replay":
        outcome = classified_replay(rt, dlq)
    else:
        raise ValueError(name)

    intended = len(scenario.recoverable_ids)
    recovered = sum(
        1 for eid in scenario.recoverable_ids if rt.completion.is_event_done(eid)
    )
    duplicates = rt.ledger.duplicate_effects()
    poison_redeliveries = sum(
        1 for eid in outcome["deliveries"] if eid in scenario.poison_ids
    )
    return {
        "strategy": name,
        "dlq_after_live": len(dlq),
        "recovered": recovered,
        "intended_effects": intended,
        "recovery_rate": round(recovered / intended, 4) if intended else 0.0,
        "duplicate_effects": duplicates,
        "duplicate_effect_rate": round(duplicates / intended, 4) if intended else 0.0,
        "poison_redeliveries": poison_redeliveries,
        "replay_deliveries": len(outcome["deliveries"]),
        "resolved_without_replay": outcome.get("resolved", 0),
        "quarantined": outcome.get("quarantined", 0),
        "remaining_dlq": outcome["remaining_dlq"],
        "handler_invocations_live": live_invocations,
        "handler_invocations_total": rt.handler_invocations,
    }


STRATEGIES = ["naive_replay", "no_replay", "classified_replay"]


def run_benchmark(seeds: list[int], n: int) -> dict:
    per_seed: dict[str, dict] = {}
    for seed in seeds:
        scenario = Scenario(seed, n)
        per_seed[str(seed)] = {
            name: run_strategy(name, Scenario(seed, n)) for name in STRATEGIES
        }
    aggregate: dict[str, dict] = {}
    for name in STRATEGIES:
        rows = [per_seed[str(s)][name] for s in seeds]
        keys = [k for k in rows[0] if isinstance(rows[0][k], (int, float))
                and k != "strategy"]
        aggregate[name] = {
            k: round(sum(r[k] for r in rows) / len(rows), 4) for k in keys
        }
    return {
        "config": {"seeds": seeds, "events_per_seed": n,
                   "max_attempts": 3, "naive_sweeps": 3},
        "per_seed": per_seed,
        "aggregate_mean": aggregate,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", type=int, nargs="+", default=[7, 19, 42])
    parser.add_argument("--n", type=int, default=600)
    parser.add_argument("--out", default="results.json")
    args = parser.parse_args()

    results = run_benchmark(args.seeds, args.n)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2)

    print(f"{'strategy':<20} {'recovery':>9} {'dup_eff':>8} "
          f"{'dup_rate':>9} {'poison_re':>10} {'invocations':>12}")
    for name in STRATEGIES:
        agg = results["aggregate_mean"][name]
        print(f"{name:<20} {agg['recovery_rate']:>9.4f} "
              f"{agg['duplicate_effects']:>8.1f} "
              f"{agg['duplicate_effect_rate']:>9.4f} "
              f"{agg['poison_redeliveries']:>10.1f} "
              f"{agg['handler_invocations_total']:>12.1f}")
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()

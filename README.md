# onceflow

**Dead-letter forensics and safe replay for event-triggered automation.**

Every event pipeline eventually fills a dead-letter queue (DLQ). The
standard recovery move — "just replay them" — is how customers get
charged twice: some of those letters failed *after* their effect had
already landed, and replaying them re-applies it. The other standard
move — never replay — silently abandons every recoverable job.

onceflow takes the third path: **classify why each execution failed,
then replay only the classes that are safe to replay, with the original
idempotency key preserved.**

## How it works

```
webhook / queue ──> intake ──> idempotent executor ──> effect ledger
                                    │ failure
                                    ▼
                              dead-letter queue
                                    │
                          forensic classifier
              ┌──────────────┬──────┴───────┬────────────────┐
        already applied    poison /      dependency down   transient /
        (effect in ledger)  scope error   (probe healthy?)  ordering
              │               │               │               │
           resolve         quarantine        replay          replay
        (no re-execution)  (never auto)   (original key)  (original key)
```

The classifier reads the failure record plus three probes: the effect
ledger (did the effect already land under the original key?), payload
validity (poison never heals), scope authorisation (replay cannot
change an authorisation fact), and dependency health. Anything it
cannot explain is quarantined, never replayed on a guess. Replays run
through a reconciling executor that checks the effect ledger before
applying, so a replay can never double-apply an effect that actually
succeeded. Every decision lands in an append-only audit log.

## Measured results

Deterministic fault-injection benchmark (600 events/seed, seeds 7/19/42;
duplicate deliveries, out-of-order chains, mid-execution crashes after
the effect, a dependency outage, poison payloads, scope errors). All
three strategies share the identical live phase; 340 of 600 events
dead-letter during the incident, 510 are recoverable.

| strategy | recovery rate | duplicate effects | duplicate-effect rate | poison re-deliveries | handler invocations |
|---|---|---|---|---|---|
| naive replay (republish all, fresh keys) | 1.0000 | 60 | 0.1176 | 180 | 1680 |
| no replay (DLQ is terminal) | 0.5098 | 0 | 0.0000 | 0 | 1160 |
| **onceflow classified replay** | **1.0000** | **0** | **0.0000** | **0** | **1350** |

Naive replay recovers everything — and double-applies every one of the
60 crash-after-effect events (11.76% of intended effects), while
re-delivering each poison payload on every sweep. No-replay is safe
and useless: half the recoverable work stays dead. Classified replay
recovers everything naive does, resolves the 60 crash victims from
the ledger *without re-executing them*, quarantines all 90 poison and
scope failures untouched, and does it with 330 fewer handler
invocations than naive. Full numbers: `results.json`; method and
honest limits: `PAPER.md`.

## Quickstart

```bash
pip install -e .
python -m onceflow.bench --seeds 7 19 42 --n 600 --out results.json
pytest
```

Webhook intake is a tiny local HTTP handler (`onceflow.serve`) or a
direct `onceflow.ingest(queue, raw_dict)` call; queues are in-process
or file-backed via an append-only JSONL log. Intake preserves a
producer-supplied idempotency key verbatim — that key is the thread
safe replay pulls on.

## Where this sits next to sibling projects

- **orderstream** is about *ordering* events on the way in.
- **webhook-ledger** is about *intake* dedup of webhooks.
- **onceflow** starts where those end: after execution has *failed*.
  It owns DLQ forensics, replay classification, and duplicate-effect
  prevention during recovery.

## Layout

- `src/onceflow/intake.py` — webhook intake (HTTP + direct), key preservation
- `src/onceflow/executor.py` — the workflow handler, standard vs reconciling executor
- `src/onceflow/ledger.py` — effect ledger, completion store, audit log
- `src/onceflow/classifier.py` — the forensic classifier
- `src/onceflow/replay.py` — live phase + the three recovery strategies
- `src/onceflow/bench.py` — the fault-injection benchmark
- `tests/` — 22 tests, including the crash-after-effect trap as a test

MIT License — Habib Ur Rehman.

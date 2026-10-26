# onceflow: Dead-Letter Forensics and Safe Replay

*Method, measured results, and honest limits.*

## The problem

An event-triggered automation pipeline — webhooks and queue messages
driving workflows — delivers at-least-once. Executions fail for
different reasons, and the failures accumulate in a dead-letter queue.
What happens next is usually one of two things, both bad:

1. **Naive replay.** An operator or script drains the DLQ back into the
   pipeline. But a DLQ entry is a record of work that failed at an
   *unknown point*. Some entries failed before doing anything; some
   failed after their side effect had already landed (the worker
   charged the card, then crashed before writing the completion
   record). Replaying the second kind re-applies the effect: the
   classic double-charge. Naive tooling makes this worse by
   re-publishing payloads as new messages, which regenerates the
   idempotency key and blinds the consumer's own dedupe.
2. **No replay.** The DLQ is treated as terminal. Nothing is ever
   double-applied, and every recoverable job — every event that failed
   only because a dependency was briefly down — is silently lost.

The recovery question is therefore not "replay or not" but "**which**
letters are safe to replay, and under what identity."

## Method

onceflow splits recovery into a forensic step and a policy step.

**Forensic classification.** Each DLQ entry carries a failure record
(error type, phase, attempts). The classifier combines it with three
probes:

- *Effect-ledger probe:* does the effect ledger already hold an effect
  under the event's **original idempotency key**? If yes, the failure
  happened after the effect (e.g. crash before the completion record)
  and the entry is **resolved without re-execution**.
- *Payload probe:* does the payload still fail schema validation? A
  poison payload fails identically forever → **quarantine**.
- *Scope probe:* is the referenced scope authorised? Authorisation is
  a fact, not a timing condition; replay cannot change it →
  **quarantine**.
- *Dependency probe:* failures caused by a down dependency are
  **replayed only once the probe reports healthy**, and deferred
  (kept in the DLQ) while it is still down.
- Remaining timing failures (transient errors, ordering violations,
  crashes where the ledger proves no effect landed) are **replayed**.
- Anything unexplained is quarantined. The classifier never guesses a
  replay.

**Safe replay.** Replays preserve the original idempotency key and run
through a reconciling executor that checks the effect ledger before
applying an effect, so even a mis-classified letter cannot double-apply
an effect that already succeeded. Replay order is per-aggregate
sequence order, so ordering failures heal instead of re-failing.
Every classification, resolution, and replay is written to an
append-only audit log.

## Experiment

A deterministic fault-injection harness (Python, CPU, no external
services). Each seed generates a 600-event stream of payment-like
workflow invocations with a fixed fault mix: 30% normal, 10% duplicate
deliveries, 15% dependency-requiring (the dependency is down for the
entire live phase, then recovers), 10% crash-after-effect, 10%
out-of-order (per-aggregate chains of 3, delivered scrambled), 10%
transient failures that exhaust live retries, 10% poison payloads,
5% scope errors. The live phase is identical for all strategies:
standard executor (completion-record dedupe only), 3 attempts, then
dead-letter. 340 of 600 events dead-letter; 510 are recoverable (all
but poison and scope).

Strategies: (a) naive replay — 3 sweeps re-publishing every remaining
DLQ payload under a fresh idempotency key, no classification;
(b) no replay; (c) onceflow classified replay.

**Metrics.** Duplicate-effect rate = effect applications beyond
exactly-once ÷ intended effects (each duplicate is a customer charged
twice). Recovery rate = recoverable events reaching completion.
Poison re-deliveries = handler deliveries of poison payloads during
the recovery phase.

## Results

Means over seeds 7, 19, 42 (per-seed rows in `results.json` are
identical; see Limits):

| strategy | recovery rate | duplicate effects | duplicate-effect rate | poison re-deliveries | scope re-deliveries | resolved w/o replay | quarantined | handler invocations |
|---|---|---|---|---|---|---|---|---|
| naive replay | 1.0000 | 60 | 0.1176 | 180 | 90 | 0 | 0 | 1680 |
| no replay | 0.5098 | 0 | 0.0000 | 0 | 0 | 0 | 0 | 1160 |
| **onceflow** | **1.0000** | **0** | **0.0000** | **0** | **0** | **60** | **90** | **1350** |

Naive replay achieves full recovery at a 11.76% duplicate-effect
rate — every crash-after-effect victim is applied twice — and burns
520 recovery deliveries, 270 of them re-delivering known-bad payloads
(180 poison + 90 scope). No replay duplicates nothing and recovers
only the 51% of work that never needed the DLQ. Classified replay
matches naive's recovery with zero duplicate effects and zero
re-deliveries of unrecoverable payloads, using 330 fewer handler
invocations than naive — because the 60 crash victims are resolved
from the ledger instead of re-executed, and the 90 hopeless letters
are never delivered again.

## Differentiation

Habib's earlier projects cover adjacent ground, deliberately disjoint
from this one: **orderstream** orders events on the way in;
**webhook-ledger** dedupes webhooks at intake. onceflow assumes both
of those have already done their jobs and starts afterwards — at the
failure record. Its subject is exclusively the DLQ/replay side:
failure classification, replay policy, and duplicate-effect prevention
during recovery.

Against the wider field: failure taxonomies for DLQ triage and
per-category replay runbooks exist as operational writing (e.g.
Anhaia's DLQ triage and replay articles on dev.to, 2026); selective
manual replay tooling exists (Warren for RabbitMQ); a multi-cloud DLQ
console with trust-gated auto-replay exists (ServiceHub); cloud
brokers offer bulk redrive (SQS). What the prior-art check did **not**
find is a runnable reference implementation that joins intake,
idempotent execution, forensic classification, and key-preserving
replay, together with a controlled fault-injection measurement of
duplicate-effect rate across the three recovery strategies. That
joined implementation and its measurement are the contribution here;
the individual ideas (classify before replay, preserve the key) are
known good practice and are credited as such.

## Honest limits

- **Simulation, not a broker.** The queue is in-process or a JSONL
  file log; no real SQS/RabbitMQ/Kafka semantics (visibility
  timeouts, partitioning, broker redrive policies) are modelled.
- **One workflow shape.** A single-effect payment-like handler.
  Multi-step workflows with partial progress across several effects
  would need per-step ledger keys; the design extends, the
  measurement does not yet.
- **The classifier is rule-based** over typed errors and probes — it
  classifies failures the handler honestly reports. A handler that
  mislabels its errors (e.g. wraps a validation bug as a transient
  error) can mislead it; the reconciling executor is the backstop
  that still prevents duplicate effects in that case.
- **The naive baseline models one common shape of naive** —
  script-based re-enqueue that regenerates message identity (the
  pattern the runbook literature warns about). A naive replay that
  preserves keys but skips classification would show fewer duplicate
  effects and the same poison re-delivery cost; the harness can be
  re-pointed at that variant.
- **Fixed fault proportions.** Per-seed results coincide because the
  mix proportions are fixed and only delivery order is shuffled;
  seeds here test order-invariance, not mix variance. Other mixes are
  one constant away (`onceflow.bench._MIX`).
- **Crash model.** Crashes are injected only immediately after the
  effect, the most dangerous point; crashes at other points are
  covered by the same ledger check but not separately measured.

## Reproduce

```bash
pip install -e .
python -m onceflow.bench --seeds 7 19 42 --n 600 --out results.json
pytest   # 22 tests
```

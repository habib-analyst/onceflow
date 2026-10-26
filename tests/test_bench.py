from onceflow.bench import run_benchmark, run_strategy, Scenario


def test_bench_is_deterministic_per_seed():
    a = run_strategy("classified_replay", Scenario(7, 120))
    b = run_strategy("classified_replay", Scenario(7, 120))
    assert a == b


def test_bench_headline_relationships_hold():
    results = run_benchmark(seeds=[7], n=120)
    rows = results["per_seed"]["7"]
    classified = rows["classified_replay"]
    naive = rows["naive_replay"]
    none = rows["no_replay"]
    # onceflow: full recovery, zero duplicate effects, zero poison
    # re-deliveries.
    assert classified["recovery_rate"] == 1.0
    assert classified["duplicate_effects"] == 0
    assert classified["poison_redeliveries"] == 0
    # naive: also recovers, but pays for it in duplicate effects and
    # poison re-deliveries.
    assert naive["recovery_rate"] == 1.0
    assert naive["duplicate_effects"] > 0
    assert naive["poison_redeliveries"] > 0
    # no replay: nothing duplicated, most recoverable work lost.
    assert none["duplicate_effects"] == 0
    assert none["recovery_rate"] < 0.6

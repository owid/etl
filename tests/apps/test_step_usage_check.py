"""Tests for the silent-breakage coverage diff used by `etl usage-check`.

`_diff_coverage` is the pure core of the checker (no I/O, no rebuild), so it's the piece worth
pinning down: it decides what counts as a downstream regression after a foundational update.
"""

from apps.step_usage_check.cli import Finding, TableCoverage, _dependency_ordered, _diff_coverage

CONSUMER = "data://garden/ns/2026-01-01/consumer"


def _cov(columns, entities=None, empty_entities=None, n_rows=10):
    return TableCoverage(
        columns=columns,
        n_rows=n_rows,
        entities=set(entities) if entities is not None else None,
        empty_entities=set(empty_entities or []),
    )


def _messages(findings: list[Finding], severity: str) -> list[str]:
    return [f.message for f in findings if f.severity == severity]


def test_no_regression_is_clean():
    before = {"t": _cov({"gdp": 10}, entities={"France", "Germany"})}
    after = {"t": _cov({"gdp": 10}, entities={"France", "Germany"})}
    assert _diff_coverage(CONSUMER, before, after) == []


def test_all_nan_column_flagged_without_baseline():
    # No baseline at all (first local build): absolute checks still fire.
    after = {"t": _cov({"gdp": 0}, entities={"France"})}
    problems = _messages(_diff_coverage(CONSUMER, None, after), "problem")
    assert any("all-NaN column" in m and "gdp" in m for m in problems)


def test_entity_present_but_empty_flagged():
    after = {
        "t": _cov(
            {"gdp": 5},
            entities={"France", "Lower-middle-income countries"},
            empty_entities={"Lower-middle-income countries"},
        )
    }
    problems = _messages(_diff_coverage(CONSUMER, None, after), "problem")
    assert any("present but all-NaN" in m and "Lower-middle-income countries" in m for m in problems)


def test_dropped_column_flagged():
    before = {"t": _cov({"gdp": 10, "pop": 10}, entities={"France"})}
    after = {"t": _cov({"gdp": 10}, entities={"France"})}
    problems = _messages(_diff_coverage(CONSUMER, before, after), "problem")
    assert any("disappeared" in m and "pop" in m for m in problems)


def test_dropped_entity_flagged():
    before = {"t": _cov({"gdp": 10}, entities={"France", "Vietnam"})}
    after = {"t": _cov({"gdp": 10}, entities={"France"})}
    problems = _messages(_diff_coverage(CONSUMER, before, after), "problem")
    assert any("entity(ies) disappeared" in m and "Vietnam" in m for m in problems)


def test_dropped_table_flagged():
    before = {"t": _cov({"gdp": 10}), "extra": _cov({"x": 3})}
    after = {"t": _cov({"gdp": 10})}
    problems = _messages(_diff_coverage(CONSUMER, before, after), "problem")
    assert any("'extra'" in m and "disappeared entirely" in m for m in problems)


def test_significant_value_loss_is_a_warning_not_a_problem():
    # A surviving column that lost >10% of its non-null values → soft warning.
    before = {"t": _cov({"gdp": 1000})}
    after = {"t": _cov({"gdp": 500})}
    findings = _diff_coverage(CONSUMER, before, after)
    assert _messages(findings, "problem") == []
    assert any("lost" in m and "gdp" in m for m in _messages(findings, "warning"))


def test_small_value_change_is_ignored():
    # A <10% drift is normal churn and should not be flagged at all.
    before = {"t": _cov({"gdp": 1000})}
    after = {"t": _cov({"gdp": 950})}
    assert _diff_coverage(CONSUMER, before, after) == []


def test_dependency_ordered_rebuilds_deps_before_dependents():
    # root -> A -> B (dag maps step -> its deps). B depends on A, A depends on root.
    dag = {
        "A": {"root"},
        "B": {"A"},
        "root": set(),
    }
    # We only rebuild the downstream consumers A and B; A must come before B so B is rebuilt
    # against the freshly-rebuilt A (a lexicographic sort already happens to give A, B here, so
    # also assert the reverse-named case below where lexicographic order is wrong).
    assert _dependency_ordered({"A", "B"}, dag) == ["A", "B"]


def test_dependency_ordered_beats_lexicographic():
    # Name the dependency later in the alphabet than its dependent, so a plain sort would put the
    # dependent first — the bug Codex caught. Dependency order must still rebuild the dep first.
    # "zzz_base" is a dep of "aaa_top"; lexicographically "aaa_top" < "zzz_base".
    dag = {
        "aaa_top": {"zzz_base"},
        "zzz_base": {"root"},
        "root": set(),
    }
    assert _dependency_ordered({"aaa_top", "zzz_base"}, dag) == ["zzz_base", "aaa_top"]


def test_dependency_ordered_is_deterministic():
    # Two consumers with no dependency between them: order among independents follows the DAG's own
    # topological order (not alphabetical), but it must be stable run-to-run for the same DAG.
    dag = {"b": {"root"}, "a": {"root"}, "root": set()}
    first = _dependency_ordered({"b", "a"}, dag)
    assert first == _dependency_ordered({"b", "a"}, dag)
    assert set(first) == {"a", "b"}

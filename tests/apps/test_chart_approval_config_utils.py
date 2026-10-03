from unittest.mock import MagicMock

from apps.chart_approval import config_utils


def test_diff_chart_dimension_data_compares_every_series_not_just_the_last(monkeypatch):
    """Regression test (caught by Codex review): a multi-series chart can have several
    dimensions sharing the same `property` (e.g. three `property: "y"` entries, one per
    series). The old dict comprehension `{d["property"]: d["variableId"] for d in ...}` kept
    only the *last* variable per property, so earlier series' changes were silently dropped
    from the comparison entirely -- with --allow-small-changes, a chart could get auto-approved
    even though an earlier, unchecked series changed by a large amount.
    """
    config_staging = {
        "dimensions": [
            {"property": "y", "variableId": 1},
            {"property": "y", "variableId": 2},
            {"property": "y", "variableId": 3},
        ]
    }
    config_prod = {
        "dimensions": [
            {"property": "y", "variableId": 10},
            {"property": "y", "variableId": 20},
            {"property": "y", "variableId": 30},
        ]
    }

    def fake_get_chart_config(chart_id, engine):
        return config_staging if engine == "engine_a" else config_prod

    # Only the pairing (1, 10) -- the *first* y series -- actually differs. If the old code
    # kept only the last dimension per property, this large change would never be compared.
    def fake_summarize(variable_id_a, env_a, variable_id_b, env_b, round_values=True, max_examples=10):
        if (variable_id_a, variable_id_b) == (1, 10):
            return {
                "variable_id_a": 1,
                "variable_id_b": 10,
                "n_common": 5,
                "n_changed": 5,
                "n_only_a": 0,
                "n_only_b": 0,
                "all_changed": [],
                "examples_changed": [],
            }
        return {
            "variable_id_a": variable_id_a,
            "variable_id_b": variable_id_b,
            "n_common": 5,
            "n_changed": 0,
            "n_only_a": 0,
            "n_only_b": 0,
            "all_changed": [],
            "examples_changed": [],
        }

    monkeypatch.setattr(config_utils, "get_chart_config", fake_get_chart_config)
    monkeypatch.setattr(config_utils, "summarize_variable_data_diff", fake_summarize)

    results = config_utils.diff_chart_dimension_data(1, "engine_a", MagicMock(), "engine_b", MagicMock())

    # All three y-series must be paired up (by position) and compared -- not just the last one.
    pairs_compared = {(r["variable_id_a"], r["variable_id_b"]) for r in results}
    assert (1, 10) in pairs_compared, "the first y-series (the one that actually changed) was dropped"
    assert len(results) == 1  # only the changed series is returned (others have n_changed == 0)


def test_diff_chart_dimension_data_flags_series_count_mismatch(monkeypatch):
    """A property whose number of dimensions changed (e.g. a series added/removed) is a
    structural change, not something to silently truncate via zip() -- it must always be
    surfaced (and therefore fail tolerance checks) rather than comparing only as many series
    as the shorter side has.
    """
    config_staging = {"dimensions": [{"property": "y", "variableId": 1}, {"property": "y", "variableId": 2}]}
    config_prod = {"dimensions": [{"property": "y", "variableId": 10}]}

    def fake_get_chart_config(chart_id, engine):
        return config_staging if engine == "engine_a" else config_prod

    monkeypatch.setattr(config_utils, "get_chart_config", fake_get_chart_config)
    monkeypatch.setattr(
        config_utils, "summarize_variable_data_diff", MagicMock(side_effect=AssertionError("should not be called"))
    )

    results = config_utils.diff_chart_dimension_data(1, "engine_a", MagicMock(), "engine_b", MagicMock())

    assert len(results) == 1
    assert results[0]["property"] == "y"
    assert results[0]["n_only_b"] > 0  # forces dimension_diff_within_tolerance to require review
    assert not config_utils.dimension_diff_within_tolerance(results[0], 1.0, 1e-6, 5)


def test_dimension_diff_within_tolerance_caps_new_points():
    """Regression test (caught by Codex review): new-coverage points (n_only_a, e.g. a fresh
    year added in staging) were let through with no limit at all. That's the right default for
    a routine update, but truly unbounded new coverage (e.g. an accidental variable swap that
    happens to look like "all points are new" against the old one) can visibly change the chart
    and deserves a human look -- so it must still be capped, just far more generously than
    max_changed_points.
    """
    base_diff = {"n_only_b": 0, "n_changed": 0, "all_changed": []}

    # A routine amount of new coverage (well under the default cap) passes.
    assert config_utils.dimension_diff_within_tolerance({**base_diff, "n_only_a": 200}, 1.0, 1e-6, 5)

    # An excessive amount of "new" coverage is rejected, using the default cap...
    assert not config_utils.dimension_diff_within_tolerance({**base_diff, "n_only_a": 5000}, 1.0, 1e-6, 5)
    # ...and an explicit, tighter cap.
    assert not config_utils.dimension_diff_within_tolerance(
        {**base_diff, "n_only_a": 200}, 1.0, 1e-6, 5, max_new_points=100
    )

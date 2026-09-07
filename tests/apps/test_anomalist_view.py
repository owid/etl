"""Tests for the ranking and rendering of stored anomalies (no database involved)."""

import pandas as pd

from apps.anomalist.view import ROW_COLUMNS, build_table, render_markdown, select_anomalies


def _scores() -> pd.DataFrame:
    """Two indicators: one with a single severe anomaly, one with many mild ones."""
    rows = [
        # indicator 1: one severe anomaly, plus three mild ones.
        ("Chad", 2023, "upgrade_change", 1, 0.9, 0.8),
        ("Niger", 2023, "upgrade_change", 1, 0.5, 0.4),
        ("Mali", 2023, "upgrade_change", 1, 0.4, 0.3),
        ("Togo", 2023, "time_change", 1, 0.3, 0.2),
        # indicator 2: many mild anomalies, none of them severe.
        ("France", 2022, "time_change", 2, 0.6, 0.5),
        ("Spain", 2022, "time_change", 2, 0.5, 0.45),
        # indicator 3: below any threshold we test with.
        ("Peru", 2021, "gp_outlier", 3, 0.05, 0.05),
    ]
    return pd.DataFrame(rows, columns=["entity_name", "year", "type", "indicator_id", "score", "score_weighted"])


def _indicators() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "indicator_id": [1, 2, 3],
            "indicator": ["maternal_mortality", "gdp_per_capita", "forest_area"],
            "indicator_name": ["Maternal mortality ratio", "GDP per capita", "Forest area"],
            "unit": ["per 100,000 live births", "international-$", "hectares"],
        }
    )


def test_groups_rank_by_worst_anomaly_not_by_count() -> None:
    df = _scores()
    df["views"] = float("nan")

    _, group_stats = select_anomalies(df, group_by="indicator", top=10, rows_per_group=5)

    # Indicator 1 has the single worst anomaly, so it outranks indicator 2 even though the mild
    # anomalies of indicator 2 are all fairly relevant.
    assert list(group_stats.index) == [1, 2, 3]
    assert group_stats.loc[1, "n_rows"] == 4


def test_rows_per_group_caps_rows_but_keeps_the_full_count() -> None:
    df = _scores()
    df["views"] = float("nan")

    selected, group_stats = select_anomalies(df, group_by="indicator", top=10, rows_per_group=2)

    # Only the two worst rows of indicator 1 are selected, but the group still knows there are 4.
    assert len(selected[selected["indicator_id"] == 1]) == 2
    assert group_stats.loc[1, "n_rows"] == 4
    assert set(selected[selected["indicator_id"] == 1]["entity_name"]) == {"Chad", "Niger"}


def test_min_score_drops_low_scoring_anomalies() -> None:
    df = _scores()
    df["views"] = float("nan")

    selected, group_stats = select_anomalies(df, group_by="indicator", min_score=0.25)

    assert 3 not in group_stats.index
    assert "Peru" not in set(selected["entity_name"])
    # Togo scores 0.2 on the weighted score, so it goes too, while its indicator stays.
    assert "Togo" not in set(selected["entity_name"])
    assert group_stats.loc[1, "n_rows"] == 3


def test_markdown_reports_hidden_rows_and_resolves_names() -> None:
    df = _scores()
    df["views"] = 1234.0

    selected, group_stats = select_anomalies(df, group_by="indicator", top=1, rows_per_group=2)
    table = build_table(selected, _indicators())
    out = render_markdown(header=["# header"], table=table, group_stats=group_stats, group_by="indicator", top=1)

    # The indicator is identified by title, id and unit, and the group key is not repeated in rows.
    assert "## Maternal mortality ratio [1] · per 100,000 live births · views14d=1,234 · 4 entities flagged" in out
    assert "Chad,2023,upgrade_change,90,80" in out
    # Scores are printed as 0-100 integers, and the capped rows are counted with their score band.
    assert "… and 2 more, relevance ≤ 40" in out
    # Only the top group is rendered.
    assert "GDP per capita" not in out


def test_markdown_groups_by_entity() -> None:
    df = _scores()
    df["views"] = float("nan")

    selected, group_stats = select_anomalies(df, group_by="entity", top=2, rows_per_group=5)
    table = build_table(selected, _indicators())
    out = render_markdown(header=["# header"], table=table, group_stats=group_stats, group_by="entity", top=2)

    assert "## Chad · 1 indicator flagged" in out
    assert "maternal_mortality,1,2023,upgrade_change,90,80" in out
    assert ROW_COLUMNS["entity"][0] == "indicator"


def test_relevance_column_is_dropped_when_not_weighting() -> None:
    """Without the weighting, `relevance` equals `anomaly` — printing both wastes tokens."""
    df = _scores()
    df["views"] = float("nan")
    df["score_weighted"] = df["score"]

    selected, group_stats = select_anomalies(df, group_by="indicator", top=1, rows_per_group=2)
    table = build_table(selected, _indicators())
    out = render_markdown(
        header=["# header"], table=table, group_stats=group_stats, group_by="indicator", top=1, relevance=False
    )

    assert "Chad,2023,upgrade_change,90\n" in out
    assert "… and 2 more, anomaly ≤ 50" in out


def test_unknown_indicator_ids_survive_rendering() -> None:
    """An indicator can be missing from `variables` (e.g. deleted after detection)."""
    df = _scores()
    df["views"] = float("nan")

    selected, group_stats = select_anomalies(df, group_by="indicator", top=1, rows_per_group=1)
    table = build_table(selected, _indicators().iloc[0:0])
    out = render_markdown(header=["# header"], table=table, group_stats=group_stats, group_by="indicator", top=1)

    assert "## 1 [1]" in out

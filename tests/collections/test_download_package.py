"""Tests for etl.collection.download_package."""

from unittest.mock import patch

from owid.catalog import Table

from etl.collection.download_package import build_wide_table_for_collection


def _table_with_two_indistinguishable_columns() -> Table:
    """A table shaped like poverty_pip's: two columns whose `original_short_name` is
    the same, distinguished only by the table's own dimensions in the column name.
    """
    tb = Table(
        {
            "country": ["Brazil", "Brazil", "Mexico"],
            "year": [2000, 2001, 2000],
            "headcount_ratio__welfare_type_income__survey_comparability_0": [5.0, 6.0, None],
            "headcount_ratio__welfare_type_income__survey_comparability_1": [None, None, 7.0],
        }
    )
    for column in tb.columns[2:]:
        tb[column].metadata.original_short_name = "headcount_ratio"
    return tb


def test_wide_table_keeps_columns_a_single_view_cannot_distinguish():
    """Test build_wide_table_for_collection - two indicators shown under one view both survive.

    Regression test for poverty_pip. The wide column used to be named after the
    indicator's dimension-stripped short name plus the dimensions of the view showing
    it, so indicators shown together by one view collapsed onto a single name: its 34
    indicators produced 24 names, 4 of them shared by 14 indicators, and the entries
    for the losers were silently overwritten. Naming the columns by catalog path makes
    that impossible -- it is the key the indicator list is built from, so there is
    exactly one column per indicator.
    """
    tb = _table_with_two_indistinguishable_columns()
    # Both indicators are shown by the same view, with identical dimension values --
    # which is precisely why the view's dimensions cannot name them apart.
    view_dimensions = [{"indicator": "headcount_ratio", "survey_comparability": "spells"}]
    used = {
        "grapher/wb/latest/pip/poverty#headcount_ratio__welfare_type_income__survey_comparability_0": view_dimensions,
        "grapher/wb/latest/pip/poverty#headcount_ratio__welfare_type_income__survey_comparability_1": view_dimensions,
    }

    with (
        patch("etl.collection.download_package._used_indicators", return_value=used),
        patch(
            "etl.collection.download_package.CatalogDataset",
            return_value={"poverty": tb.set_index(["country", "year"])},
        ),
    ):
        wide, column_to_dimensions = build_wide_table_for_collection(collection=None)  # ty: ignore[invalid-argument-type]

    assert list(column_to_dimensions) == list(used), "one entry per indicator, in first-seen order"
    assert not wide.columns.duplicated().any(), "duplicate column names would break the CSV and Parquet"
    # Both indicators keep their own values. Rows are sorted by (country, year).
    left, right = list(used)
    assert wide["country"].tolist() == ["Brazil", "Brazil", "Mexico"]
    assert wide[left].fillna(-1).tolist() == [5.0, 6.0, -1]
    assert wide[right].fillna(-1).tolist() == [-1, -1, 7.0]


def _table_with_day_offsets() -> Table:
    """A table shaped the way grapher stores sub-yearly data: day offsets in "year"."""
    tb = Table({"country": ["Angola"] * 3, "year": [-17, 0, 14], "weekly_cases": [1.0, 2.0, 3.0]})
    tb["weekly_cases"].metadata.display = {"zeroDay": "2020-01-21", "timeInterval": "day"}
    return tb


def test_sub_yearly_offsets_decode_to_dates():
    """Test _resolve_time_column - day offsets become real dates, keyed on timeInterval.

    Grapher stores every interval shorter than a year as days-since-zeroDay integers in a
    column named "year". This used to be detected via `display.yearIsDay`, a flag the repo
    removed, so the branch could never be taken and covid's weekly cases published as
    `Year,-17` instead of 2020-01-04.
    """
    from etl.collection.download_package import _resolve_time_column

    tb, time_col = _resolve_time_column(_table_with_day_offsets())
    assert time_col == "date"
    assert tb["date"].tolist() == ["2020-01-04", "2020-01-21", "2020-02-04"]
    assert "year" not in tb.columns


def test_calendar_years_are_left_alone():
    """Test _resolve_time_column - year and decade intervals stay on the year axis.

    A decade codes a representative calendar year, not an offset, so it must not decode.
    """
    from etl.collection.download_package import _resolve_time_column

    for interval in (None, "year", "decade"):
        tb = Table({"country": ["Angola"], "year": [1850], "population": [1.0]})
        if interval:
            tb["population"].metadata.display = {"timeInterval": interval}
        out, time_col = _resolve_time_column(tb)
        assert time_col == "year", f"interval={interval}"
        assert out["year"].tolist() == [1850]


def test_table_mixing_offsets_and_years_is_refused():
    """Test _resolve_time_column - a table with both kinds of "year" value cannot be converted.

    Converting it would turn the yearly columns' calendar years into nonsense dates; not
    converting does the same to the offsets. Either way it is silent, so refuse instead.
    """
    import pytest

    from etl.collection.download_package import MixedTimeGranularityError, _resolve_time_column

    tb = Table({"country": ["Angola"], "year": [0], "weekly_cases": [1.0], "population": [2.0]})
    tb["weekly_cases"].metadata.display = {"zeroDay": "2020-01-21", "timeInterval": "day"}
    tb["population"].metadata.display = {"timeInterval": "year"}

    with pytest.raises(MixedTimeGranularityError, match="mixes sub-yearly and yearly"):
        _resolve_time_column(tb)

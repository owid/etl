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
    it, so N indicators reachable from one view collapsed onto one name: 306 indicators
    became 40 columns, and the entries for the other 266 were silently overwritten.
    Naming the columns by catalog path makes that impossible -- it is the key the
    indicator list is built from, so there is exactly one column per indicator.
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

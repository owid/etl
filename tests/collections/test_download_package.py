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
        result = build_wide_table_for_collection(collection=None)  # ty: ignore[invalid-argument-type]

    wide, column_to_dimensions = result.table, result.column_to_dimensions
    assert not result.is_stacked, "one time axis, so no frequency column"
    assert result.time_header == "Year"
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


def _annual_and_monthly_tables():
    """Two tables of the same metric at two resolutions, as electricity_mix stores them."""
    annual = Table({"country": ["Spain", "Spain"], "year": [2024, 2025], "generation": [280.9, 287.9]})
    annual["generation"].metadata.display = {"timeInterval": "year"}
    monthly = Table({"country": ["Spain", "Spain"], "date": ["2026-05-01", "2026-06-01"], "generation": [20.1, 22.2]})
    monthly["generation"].metadata.display = {"timeInterval": "month"}
    return annual, monthly


def test_mixed_frequencies_stack_with_a_frequency_key():
    """Test build_wide_table_for_collection - two time axes become rows, not columns.

    An integer year and a calendar date cannot share a time column without stamping the
    annual figures with a month and a day they do not have. Stacking keeps it to one
    table and one column per metric, with the resolution carried per row -- which also
    avoids the 50 duplicate column names electricity_mix would otherwise produce, since
    the same metric has the same name at both frequencies.
    """
    annual, monthly = _annual_and_monthly_tables()
    used = {
        "grapher/energy/latest/em/annual#generation": [{"frequency": "annual"}],
        "grapher/energy/latest/em/monthly#generation": [{"frequency": "monthly"}],
    }
    datasets = {
        "annual": annual.set_index(["country", "year"]),
        "monthly": monthly.set_index(["country", "date"]),
    }

    with (
        patch("etl.collection.download_package._used_indicators", return_value=used),
        patch("etl.collection.download_package.CatalogDataset", return_value=datasets),
    ):
        result = build_wide_table_for_collection(collection=None)  # ty: ignore[invalid-argument-type]

    assert result.is_stacked
    assert result.time_header == "Time"
    assert result.key_columns == ["country", "frequency", "time"]
    # Labels are written at the resolution the data has -- no padded-out days.
    assert sorted(result.table["time"]) == ["2024", "2025", "2026-05", "2026-06"]
    assert sorted(set(result.table["frequency"])) == ["annual", "monthly"]
    assert result.column_to_frequency == {
        "grapher/energy/latest/em/annual#generation": "annual",
        "grapher/energy/latest/em/monthly#generation": "monthly",
    }
    # Every row belongs to exactly one frequency, so the metric columns never overlap.
    paths = list(used)
    both = result.table[paths[0]].notna() & result.table[paths[1]].notna()
    assert not both.any()


def test_same_metric_at_two_frequencies_merges_into_one_column():
    """Test _merge_columns_by_name - one column, rows distinguished by frequency.

    Merging is only safe because the rows are disjoint; the helper asserts that rather
    than trusting it, since quietly keeping one of two overlapping values is exactly the
    failure this module has produced before.
    """
    from etl.collection.download_package import _merge_columns_by_name

    annual, monthly = _annual_and_monthly_tables()
    used = {
        "grapher/energy/latest/em/annual#generation": [{"frequency": "annual"}],
        "grapher/energy/latest/em/monthly#generation": [{"frequency": "monthly"}],
    }
    datasets = {
        "annual": annual.set_index(["country", "year"]),
        "monthly": monthly.set_index(["country", "date"]),
    }
    with (
        patch("etl.collection.download_package._used_indicators", return_value=used),
        patch("etl.collection.download_package.CatalogDataset", return_value=datasets),
    ):
        result = build_wide_table_for_collection(collection=None)  # ty: ignore[invalid-argument-type]

    col = type("C", (), {"meta": {"name": "Electricity generation - TWh"}})()
    indicators = [(path, "Electricity generation - TWh", i, col) for i, path in enumerate(used)]
    merged = _merge_columns_by_name(result.table, indicators, result.column_to_dimensions)

    assert len(merged) == 1, "one metric, one column"
    assert merged[0].name == "Electricity generation - TWh"
    assert merged[0].values.notna().sum() == 4, "all four observations survive the merge"
    # Both views are reported, so a consumer can still see it spans two frequencies.
    assert merged[0].combinations == [{"frequency": "annual"}, {"frequency": "monthly"}]


def test_readme_strips_detail_on_demand_links():
    """Test _readme - DoD links are removed from the whole document.

    `[terawatt-hours](#dod:watt-hours)` is a tooltip on our site and a dead link in a
    downloaded file. metadata.json has always been stripped (assembleMetadata does it);
    the readme was not, so electricity-mix shipped 33 of them.
    """
    from etl.collection.download_package import _readme

    section = "## Total energy supply\nMeasured in [terawatt-hours](#dod:watt-hours) of energy."
    out = _readme("Energy mix", "https://ourworldindata.org/grapher/energy-mix", [section])

    assert "#dod:" not in out
    assert "Measured in terawatt-hours of energy." in out, "the label survives, only the link goes"


def test_zip_entries_are_stamped_with_the_build_date():
    """Test _zip_timestamp - entries carry the build date, not 1980.

    The stamp has to be fixed rather than "now", or an unchanged package would be a new
    object on every ETL run. It used to be 1980-01-01, the earliest a zip can represent,
    which is deterministic but shows up in a file listing as a date no file could have.
    The build date is fixed in the same way and is true.
    """
    from datetime import date

    from etl.collection.download_package import _zip_timestamp

    assert _zip_timestamp(date(2026, 8, 27)) == (2026, 8, 27, 0, 0, 0)
    # Same day in, same stamp out -- this is what keeps a same-day rebuild byte-identical.
    assert _zip_timestamp(date(2026, 8, 27)) == _zip_timestamp(date(2026, 8, 27))
    assert _zip_timestamp(date(2026, 8, 28)) != _zip_timestamp(date(2026, 8, 27))


def _indicator_meta() -> dict:
    """An indicator's public metadata JSON, cut down to the fields the readme reads."""
    return {
        "name": "Total electricity generation",
        "unit": "terawatt-hours",
        "timespan": "1900-2025",
        "processingLevel": "major",
        "updatePeriodDays": 365,
        "descriptionShort": "Total electricity generated, measured in terawatt-hours.",
        "descriptionKey": ["Covers utility-scale generation only."],
        "descriptionFromProducer": "Ember compiles this from national statistics.",
        "descriptionProcessing": "We stitch Ember onto the Energy Institute's earlier years.",
        "origins": [
            {
                "producer": "Ember",
                "title": "Yearly Electricity Data",
                "datePublished": "2026-04-21",
                "dateAccessed": "2026-04-24",
                "urlMain": "https://ember-energy.org/data/yearly-electricity-data/",
            }
        ],
    }


def test_indicator_section_sits_under_the_heading_that_introduces_it():
    """Test column_readme_text - per-indicator headings are one level deeper than upstream's.

    The document puts these sections under "## Detailed information about each time
    series"; upstream's levels made each indicator a sibling of that heading rather
    than a child, so the second half of the readme had no structure at all.
    """
    from datetime import date

    from etl.collection.download_package_format import IndicatorColumn, column_readme_text

    lines = column_readme_text(
        IndicatorColumn(_indicator_meta()), date(2026, 8, 27), heading="Electricity - TWh", include_sources=False
    )
    headings = [line for line in lines if line.startswith("#")]

    assert headings == [
        "### Electricity - TWh",
        "#### How to cite this data",
        "#### What you should know about this data",
        "#### How this data is described by its producers",
        "#### Notes on our processing step for this indicator",
    ]


def test_indicator_section_drops_the_full_citation_and_softens_the_update_date():
    """Test column_readme_text - the short citation stands alone, next update is expected.

    The full citation restated producer names the Sources section already gives in
    detail, and was a fifth of electricity-mix's readme. The next-update date is our
    last update plus the producer's stated period, so it is an expectation, not a date
    we can promise.
    """
    from datetime import date

    from etl.collection.download_package_format import IndicatorColumn, column_readme_text

    col = IndicatorColumn(_indicator_meta())
    text = "\n".join(column_readme_text(col, date(2026, 8, 27), heading="Electricity - TWh", include_sources=False))

    assert "Next expected update:" in text
    assert "Next update:" not in text
    assert col.citation_short() in text
    assert "[original data]" not in text, "the full citation is gone"
    assert "#### In-line citation" not in text, "nothing left to contrast the citation with"
    # The full attribution moves in with the other facts about the indicator, above the
    # citation rather than trailing it.
    assert text.index("Source: ") < text.index("#### How to cite this data")


def test_readme_describes_the_time_columns_the_package_actually_has():
    """Test _readme - the CSV section names this package's time columns, not both options.

    Upstream's readme is one static string, so it has to hedge ("either Year or Day").
    This one is built per package, and a package that mixes resolutions has a
    Frequency/Time pair that the hedge does not describe at all.
    """
    from etl.collection.download_package import _readme

    annual = _readme("Energy mix", "https://example.org", [], time_header="Year")
    assert '"Year" — the timepoint, as an integer year.' in annual
    assert "Frequency" not in annual

    stacked = _readme("Energy mix", "https://example.org", [], time_header="Time", frequency_column=True)
    assert '"Frequency" and "Time"' in stacked

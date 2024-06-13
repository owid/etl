import pytest

from owid.catalog import charts
from owid.catalog.internal import LicenseError

# NOTE: the tests below make multiple network requests per check, we could consider
#       mocking them out if they cause problems


def test_fetch_chart_data_with_slug_as_column():
    chart = charts.Chart("life-expectancy")
    df = chart.get_data()
    assert df is not None
    assert len(df) > 0
    assert "entities" in df.columns
    assert "years" in df.columns
    assert "life_expectancy" in df.columns

    assert "metadata" in df.attrs
    assert "life_expectancy" in df.attrs["metadata"]


def test_fetch_chart_data_with_multiple_indicators():
    df = charts.Chart("eat-lancet-diet-comparison").get_data()
    value_cols = df.columns.difference(["entities", "years"])
    assert len(value_cols) > 1

    assert "metadata" in df.attrs
    assert all(c in df.attrs["metadata"] for c in value_cols)


def test_fetch_non_redistributable_chart():
    # a chart where nonRedistributable is true in the indicator's metadata; see also
    # the dataset at https://admin.owid.io/admin/datasets/6457
    chart = charts.Chart("test-scores-ai-capabilities-relative-human-performance")
    with pytest.raises(LicenseError):
        chart.get_data()


def test_list_charts():
    slugs = charts.list_charts()
    assert len(slugs) > 0
    assert "life-expectancy" in slugs

    # all unique
    assert len(slugs) == len(set(slugs))


def test_fetch_missing_chart():
    with pytest.raises(charts.ChartNotFoundError):
        charts.Chart("this-chart-does-not-exist").bundle


def test_fetch_chart_with_dates():
    df = charts.get_data("sea-level")
    assert "dates" in df.columns
    assert "years" not in df.columns
    assert len(df) > 0


def test_fetch_chart_by_url():
    df = charts.get_data("https://ourworldindata.org/grapher/life-expectancy")
    assert set(df.columns) == set(["entities", "years", "life_expectancy"])
    assert len(df) > 0


def test_fetch_chart_by_non_grapher_url():
    with pytest.raises(ValueError):
        charts.get_data("https://ourworldindata.org/this-is-not-a-grapher-url")


def test_fetch_chart_by_missing_grapher_url():
    with pytest.raises(charts.ChartNotFoundError):
        charts.get_data("https://ourworldindata.org/grapher/this-chart-does-not-exist")

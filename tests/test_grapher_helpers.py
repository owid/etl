from unittest import mock

import numpy as np
import pandas as pd
import pytest
from owid.catalog import (
    DatasetMeta,
    Table,
    TableMeta,
    VariableMeta,
)

from etl.grapher import helpers as gh


def test_yield_wide_table():
    df = pd.DataFrame(
        {
            "year": [2019, 2020, 2021],
            "entityId": [1, 2, 3],
            "_1": [1, 2, 3],
            "a__pct": [1, 2, 3],
        }
    )
    table = Table(df.set_index(["entityId", "year"]))
    table._1.metadata.unit = "kg"
    table.a__pct.metadata.unit = "pct"

    tables = list(gh._yield_wide_table(table))

    assert tables[0].reset_index().to_dict(orient="list") == {
        "_1": [1, 2, 3],
        "entityId": [1, 2, 3],
        "year": [2019, 2020, 2021],
    }
    assert tables[0].metadata.short_name == "_1"
    assert tables[0]["_1"].metadata.unit == "kg"

    assert tables[1].reset_index().to_dict(orient="list") == {
        "a__pct": [1, 2, 3],
        "entityId": [1, 2, 3],
        "year": [2019, 2020, 2021],
    }
    assert tables[1].metadata.short_name == "a__pct"
    assert tables[1]["a__pct"].metadata.unit == "pct"


def test_yield_wide_table_with_dimensions():
    df = pd.DataFrame(
        {
            "year": [2019, 2019, 2019, 2019],
            "entityId": [1, 1, 1, 1],
            "age": ["10-18", "19-25", "19-25", np.nan],
            "deaths": [1, 2, 3, 4],
        }
    )
    table = Table(df.set_index(["entityId", "year", "age"]))
    table.deaths.metadata.unit = "people"
    table.deaths.metadata.title = "Deaths"
    grapher_tables = list(gh._yield_wide_table(table))

    assert len(grapher_tables) == 3

    t = grapher_tables[0]
    assert t.columns[0] == "deaths__age_10_18"
    assert t[t.columns[0]].metadata.title == "Deaths - Age: 10-18"

    t = grapher_tables[1]
    assert t.columns[0] == "deaths__age_19_25"
    assert t[t.columns[0]].metadata.title == "Deaths - Age: 19-25"

    t = grapher_tables[2]
    assert t.columns[0] == "deaths"
    assert t[t.columns[0]].metadata.title == "Deaths"


def test_long_to_wide_tables():
    deaths_meta = VariableMeta(title="Deaths", unit="people")
    births_meta = VariableMeta(title="Births", unit="people")

    long = pd.DataFrame(
        {
            "year": [2019, 2019, 2019, 2019],
            "entity_id": [1, 1, 1, 1],
            "variable": ["deaths", "deaths", "births", "births"],
            "meta": [deaths_meta, deaths_meta, births_meta, births_meta],
            "value": [1, 2, 3, 4],
            "sex": ["male", "female", "male", "female"],
        }
    ).set_index(["year", "entity_id", "sex"])
    table = Table(long, metadata=TableMeta(dataset=DatasetMeta()))
    grapher_tables = list(gh.long_to_wide_tables(table))

    t = grapher_tables[0]
    assert t.index.names == ["year", "entity_id", "sex"]
    assert t.columns[0] == "births"
    assert t[t.columns[0]].metadata.title == "Births"

    t = grapher_tables[1]
    assert t.index.names == ["year", "entity_id", "sex"]
    assert t.columns[0] == "deaths"
    assert t[t.columns[0]].metadata.title == "Deaths"


def test_contains_inf():
    assert gh.contains_inf(pd.Series([1, np.inf]))
    assert not gh.contains_inf(pd.Series([1, 2]))
    assert not gh.contains_inf(pd.Series(["a", 2]))
    assert not gh.contains_inf(pd.Series(["a", "b"]))
    assert not gh.contains_inf(pd.Series(["a", "b"]).astype("category"))


def _sample_table() -> Table:
    table = Table(
        pd.DataFrame(
            {
                "deaths": [0, 1],
                "year": [2019, 2020],
                "country": ["France", "Poland"],
                "sex": ["female", "male"],
            }
        )
    )
    table.metadata.dataset = DatasetMeta(description="Dataset description")
    table.metadata.description = "Table description"
    return table


def test_adapt_table_for_grapher_multiindex():
    with mock.patch("etl.grapher.helpers._get_entities_from_db") as mock_get_entities_from_db:
        with mock.patch("etl.grapher.io._fetch_entities") as mock_fetch_entities:
            mock_get_entities_from_db.return_value = {"Poland": 1, "France": 2}
            mock_fetch_entities.return_value = pd.DataFrame(
                {
                    "entityId": [1, 2],
                    "entityName": ["Poland", "France"],
                    "entityCode": ["PL", "FR"],
                }
            )

            engine = mock.Mock()

            table = _sample_table()
            out_table = gh._adapt_table_for_grapher(table, engine)
            assert out_table.index.names == ["entityId", "entityCode", "entityName", "year"]
            assert out_table.columns.tolist() == ["deaths", "sex"]

            table = _sample_table().set_index(["country", "year", "sex"])
            out_table = gh._adapt_table_for_grapher(table, engine)
            assert out_table.index.names == ["entityId", "entityCode", "entityName", "year", "sex"]
            assert out_table.columns.tolist() == ["deaths"]

            table = _sample_table().set_index(["sex"])
            out_table = gh._adapt_table_for_grapher(table, engine)
            assert out_table.index.names == ["entityId", "entityCode", "entityName", "year", "sex"]
            assert out_table.columns.tolist() == ["deaths"]


def test_underscore_column_and_dimensions():
    short_name = "a" * 200
    dims = {"age": "1" * 100}
    expected = short_name + "__age_1111111111111111_4e8d3bae4e8b9786396245429a8430af"
    assert gh._underscore_column_and_dimensions(short_name, dims, trim_long_short_name=True) == expected


def test_title_column_and_dimensions():
    assert gh._title_column_and_dimensions("A", {"age": "1"}) == "A - Age: 1"
    assert gh._title_column_and_dimensions("A", {"age_group": "15-18"}) == "A - Age group: 15-18"


def test_long_to_wide():
    df = pd.DataFrame(
        {
            "year": [2019, 2019, 2019, 2019],
            "country": ["France", "France", "France", "France"],
            "age": ["10-18", "19-25", "26-30", np.nan],
            "deaths": [1, 2, 3, 4],
        }
    )
    table = Table(df.set_index(["country", "year", "age"]))
    table.deaths.metadata.unit = "people"
    table.deaths.metadata.title = "Deaths"
    # table.deaths.metadata.presentation = {"grapher_config": {"colorScale": {"customNumericValues": "[1, 2, 3]"}}}

    wide = gh.long_to_wide(table)

    assert list(wide.columns) == ["deaths", "deaths__age_10_18", "deaths__age_19_25", "deaths__age_26_30"]

    assert wide["deaths"].m.title == "Deaths"
    assert wide["deaths__age_10_18"].m.title == "Deaths - Age: 10-18"
    assert wide["deaths__age_19_25"].m.title == "Deaths - Age: 19-25"
    assert wide["deaths__age_26_30"].m.title == "Deaths - Age: 26-30"


def _dated_table() -> Table:
    df = pd.DataFrame(
        {
            "country": ["France", "France", "France"],
            "date": ["2020-01-01", "2020-02-01", "2020-03-01"],
            "value": [1.0, 2.0, 3.0],
        }
    )
    return Table(df)


def test_adapt_table_with_dates_defaults_to_day():
    tb = gh.adapt_table_with_dates_to_grapher(_dated_table())
    assert tb["value"].m.display["timeInterval"] == "day"
    assert tb["value"].m.display["zeroDay"] == "2020-01-01"
    # Dates become days-since-zeroDay integers, whatever the interval.
    assert tb["year"].tolist() == [0, 31, 60]


def test_adapt_table_with_dates_explicit_interval_wins():
    tb = _dated_table()
    tb["value"].metadata.display = {"timeInterval": "week"}
    tb = gh.adapt_table_with_dates_to_grapher(tb, time_interval="month")
    assert tb["value"].m.display["timeInterval"] == "month"


def test_adapt_table_with_dates_preserves_declared_interval():
    # Grapher steps call this implicitly with no time_interval; an interval declared in metadata
    # must survive rather than being overwritten with "day".
    tb = _dated_table()
    tb["value"].metadata.display = {"timeInterval": "month"}
    tb = gh.adapt_table_with_dates_to_grapher(tb)
    assert tb["value"].m.display["timeInterval"] == "month"


class _FakeDataset:
    """Minimal stand-in for catalog.Dataset: grapher_checks only needs a title and its tables."""

    def __init__(self, tables):
        self.metadata = DatasetMeta(title="Test dataset")
        self._tables = tables

    def __iter__(self):
        return iter(self._tables)


def test_grapher_checks_rejects_missing_year():
    # A row with no year is not plottable, and its NA used to survive all the way to the MySQL
    # upsert, where it failed with an unattributable "boolean value of NA is ambiguous".
    df = pd.DataFrame(
        {
            "country": ["France", "France"],
            "year": pd.array([2020, None], dtype="Int64"),
            "value": [1, 2],
        }
    )
    tb = Table(df.set_index(["country", "year"]), short_name="tb")

    with pytest.raises(AssertionError, match="1 row\\(s\\) with a missing `year`"):
        gh.grapher_checks(_FakeDataset([tb]))


def test_grapher_checks_rejects_missing_date():
    df = pd.DataFrame(
        {
            "country": ["France", "France"],
            "date": pd.to_datetime(pd.Series(["2020-01-01", None])),
            "value": [1, 2],
        }
    )
    tb = Table(df.set_index(["country", "date"]), short_name="tb")

    with pytest.raises(AssertionError, match="1 row\\(s\\) with a missing `date`"):
        gh.grapher_checks(_FakeDataset([tb]))

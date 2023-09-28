from unittest import mock

import numpy as np
import pandas as pd
from owid.catalog import DatasetMeta, Source, Table, TableMeta, VariableMeta

from etl import grapher_helpers as gh


def test_yield_wide_table():
    df = pd.DataFrame(
        {
            "year": [2019, 2020, 2021],
            "entity_id": [1, 2, 3],
            "_1": [1, 2, 3],
            "a__pct": [1, 2, 3],
        }
    )
    table = Table(df.set_index(["entity_id", "year"]))
    table._1.metadata.unit = "kg"
    table.a__pct.metadata.unit = "pct"

    tables = list(gh._yield_wide_table(table))

    assert tables[0].reset_index().to_dict(orient="list") == {
        "_1": [1, 2, 3],
        "entity_id": [1, 2, 3],
        "year": [2019, 2020, 2021],
    }
    assert tables[0].metadata.short_name == "_1"
    assert tables[0]["_1"].metadata.unit == "kg"

    assert tables[1].reset_index().to_dict(orient="list") == {
        "a__pct": [1, 2, 3],
        "entity_id": [1, 2, 3],
        "year": [2019, 2020, 2021],
    }
    assert tables[1].metadata.short_name == "a__pct"
    assert tables[1]["a__pct"].metadata.unit == "pct"


def test_yield_wide_table_with_dimensions():
    df = pd.DataFrame(
        {
            "year": [2019, 2019, 2019],
            "entity_id": [1, 1, 1],
            "age": ["10-18", "19-25", "19-25"],
            "deaths": [1, 2, 3],
        }
    )
    table = Table(df.set_index(["entity_id", "year", "age"]))
    table.deaths.metadata.unit = "people"
    table.deaths.metadata.title = "Deaths"
    grapher_tables = list(gh._yield_wide_table(table, dim_titles=["Age group"]))

    t = grapher_tables[0]
    assert t.columns[0] == "deaths__age_10_18"
    assert t[t.columns[0]].metadata.title == "Deaths - Age group: 10-18"

    t = grapher_tables[1]
    assert t.columns[0] == "deaths__age_19_25"
    assert t[t.columns[0]].metadata.title == "Deaths - Age group: 19-25"


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
    table = Table(long, metadata=TableMeta(dataset=DatasetMeta(sources=[Source()])))
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


def test_ensure_source_per_variable_multiple_sources():
    table = Table(
        pd.DataFrame(
            {
                "deaths": [0, 1],
            }
        )
    )
    table.metadata.dataset = DatasetMeta(
        description="Dataset description", sources=[Source(name="s3", description="s3 description")]
    )
    table.metadata.description = "Table description"

    # multiple sources
    table.deaths.metadata.sources = [
        Source(name="s1", description="s1 description"),
        Source(name="s2", description="s2 description"),
    ]
    new_table = gh._ensure_source_per_variable(table)
    assert len(new_table.deaths.metadata.sources) == 1
    source = new_table.deaths.metadata.sources[0]
    assert source.name == "s1; s2"
    assert source.description == "s1 description\ns2 description"

    # no sources
    table.deaths.metadata.sources = []
    new_table = gh._ensure_source_per_variable(table)
    assert len(new_table.deaths.metadata.sources) == 1
    source = new_table.deaths.metadata.sources[0]
    assert source.name == "s3"
    assert source.description == "Dataset description\ns3 description"

    # sources have no description, but table has
    table.deaths.metadata.sources = [Source(name="s1")]
    new_table = gh._ensure_source_per_variable(table)
    assert len(new_table.deaths.metadata.sources) == 1
    source = new_table.deaths.metadata.sources[0]
    assert source.name == "s1"
    assert source.description == "Table description"


def test_combine_metadata_sources():
    sources = [
        Source(name="s1", description="s1 description"),
        Source(name="s2", description="s2 description"),
    ]
    source = gh.combine_metadata_sources(sources)
    assert source.name == "s1; s2"
    assert source.description == "s1 description\ns2 description"

    # make sure we haven't modified original sources
    assert sources[0].name == "s1"


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
    table.metadata.dataset = DatasetMeta(
        description="Dataset description", sources=[Source(name="s3", description="s3 description")]
    )
    table.metadata.description = "Table description"
    return table


def test_adapt_table_for_grapher_multiindex():
    with mock.patch("etl.grapher_helpers._get_entities_from_db") as mock_get_entities_from_db:
        mock_get_entities_from_db.return_value = {"Poland": 1, "France": 2}

        table = _sample_table()
        out_table = gh._adapt_table_for_grapher(table)
        assert out_table.index.names == ["entity_id", "year"]
        assert out_table.columns.tolist() == ["deaths", "sex"]

        table = _sample_table().set_index(["country", "year", "sex"])
        out_table = gh._adapt_table_for_grapher(table)
        assert out_table.index.names == ["entity_id", "year", "sex"]
        assert out_table.columns.tolist() == ["deaths"]

        table = _sample_table().set_index(["sex"])
        out_table = gh._adapt_table_for_grapher(table)
        assert out_table.index.names == ["entity_id", "year", "sex"]
        assert out_table.columns.tolist() == ["deaths"]

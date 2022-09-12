import numpy as np
import pandas as pd
import pytest
from owid.catalog import DatasetMeta, Source, Table, TableMeta, VariableMeta

from etl.grapher_helpers import (
    _ensure_source_per_variable,
    _yield_long_table,
    _yield_wide_table,
    combine_metadata_sources,
    contains_inf,
)


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

    tables = list(yield_wide_table(table))

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
    grapher_tables = list(_yield_wide_table(table, dim_titles=["Age group"]))

    t = grapher_tables[0]
    assert t.columns[0] == "deaths__age_10_18"
    assert t[t.columns[0]].metadata.title == "Deaths - Age group: 10-18"

    t = grapher_tables[1]
    assert t.columns[0] == "deaths__age_19_25"
    assert t[t.columns[0]].metadata.title == "Deaths - Age group: 19-25"


def test_yield_long_table_with_dimensions():
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
    grapher_tables = list(_yield_long_table(table, dim_titles=["Sex"]))

    t = grapher_tables[0]
    assert t.columns[0] == "births__sex_female"
    assert t[t.columns[0]].metadata.title == "Births - Sex: female"

    t = grapher_tables[1]
    assert t.columns[0] == "births__sex_male"
    assert t[t.columns[0]].metadata.title == "Births - Sex: male"

    t = grapher_tables[2]
    assert t.columns[0] == "deaths__sex_female"
    assert t[t.columns[0]].metadata.title == "Deaths - Sex: female"

    t = grapher_tables[3]
    assert t.columns[0] == "deaths__sex_male"
    assert t[t.columns[0]].metadata.title == "Deaths - Sex: male"


def test_yield_long_table_with_dimensions_error():
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
    table = Table(long)  # no metadata with sources
    with pytest.raises(AssertionError):
        _ = list(_yield_long_table(table, dim_titles=["Sex"]))


def test_contains_inf():
    assert contains_inf(pd.Series([1, np.inf]))
    assert not contains_inf(pd.Series([1, 2]))
    assert not contains_inf(pd.Series(["a", 2]))
    assert not contains_inf(pd.Series(["a", "b"]))
    assert not contains_inf(pd.Series(["a", "b"]).astype("category"))


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
    new_table = _ensure_source_per_variable(table)
    assert len(new_table.deaths.metadata.sources) == 1
    source = new_table.deaths.metadata.sources[0]
    assert source.name == "s1; s2"
    assert source.description == "s1 description\ns2 description"

    # no sources
    table.deaths.metadata.sources = []
    new_table = _ensure_source_per_variable(table)
    assert len(new_table.deaths.metadata.sources) == 1
    source = new_table.deaths.metadata.sources[0]
    assert source.name == "s3"
    assert source.description == "Dataset description\ns3 description"

    # sources have no description, but table has
    table.deaths.metadata.sources = [Source(name="s1")]
    new_table = _ensure_source_per_variable(table)
    assert len(new_table.deaths.metadata.sources) == 1
    source = new_table.deaths.metadata.sources[0]
    assert source.name == "s1"
    assert source.description == "Table description"


def test_combine_metadata_sources():
    sources = [
        Source(name="s1", description="s1 description"),
        Source(name="s2", description="s2 description"),
    ]
    source = combine_metadata_sources(sources)
    assert source.name == "s1; s2"
    assert source.description == "s1 description\ns2 description"

    # make sure we haven't modified original sources
    assert sources[0].name == "s1"

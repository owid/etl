from unittest import mock

import numpy as np
import pandas as pd
from owid.catalog import (
    DatasetMeta,
    Source,
    Table,
    TableMeta,
    VariableMeta,
    VariablePresentationMeta,
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
    with mock.patch("etl.grapher.helpers._get_entities_from_db") as mock_get_entities_from_db:
        with mock.patch("etl.grapher_io._fetch_entities") as mock_fetch_entities:
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


def test_expand_jinja():
    m = VariableMeta(
        title="Title << foo >>",
        description_key=[
            '<% if foo == "bar" %>This is bar<% else %>This is not bar<% endif %>',
        ],
        presentation=VariablePresentationMeta(
            title_variant="Variant << foo >>",
        ),
        display={
            "isProjection": "<% if foo == 'bar' %>true<% else %>false<% endif %>",
        },
    )
    out = gh._expand_jinja(m, dim_dict={"foo": "bar"})
    assert out.to_dict() == {
        "title": "Title bar",
        "description_key": ["This is bar"],
        "presentation": {"title_variant": "Variant bar"},
        "display": {"isProjection": True},
    }


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

    wide = gh.long_to_wide(table)

    assert list(wide.columns) == ["deaths", "deaths__age_10_18", "deaths__age_19_25", "deaths__age_26_30"]

    assert wide["deaths"].m.title == "Deaths"
    assert wide["deaths__age_10_18"].m.title == "Deaths - Age: 10-18"
    assert wide["deaths__age_19_25"].m.title == "Deaths - Age: 19-25"
    assert wide["deaths__age_26_30"].m.title == "Deaths - Age: 26-30"

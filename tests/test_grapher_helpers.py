import pandas as pd

from owid.catalog import Table, VariableMeta
from etl.grapher_helpers import yield_wide_table, yield_long_table


def test_yield_wide_table():
    df = pd.DataFrame(
        {
            "year": [2019, 2020, 2021],
            "entity_id": [1, 2, 3],
            "_1": [1, 2, 3],
        }
    )
    table = Table(df.set_index(["entity_id", "year"]))
    table._1.metadata.unit = "kg"
    grapher_tab = list(yield_wide_table(table))[0]
    assert grapher_tab.reset_index().to_dict(orient="list") == {
        "_1": [1, 2, 3],
        "entity_id": [1, 2, 3],
        "year": [2019, 2020, 2021],
    }
    assert grapher_tab.metadata.short_name == "_1"


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
    grapher_tables = list(yield_wide_table(table, dim_titles=["Age group"]))

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
    grapher_tables = list(yield_long_table(Table(long), dim_titles=["Sex"]))

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

import pandas as pd

from owid.catalog import Table
from etl.grapher_helpers import yield_wide_table


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
    assert grapher_tab.to_dict() == {"_1": {(1, 2019): 1, (2, 2020): 2, (3, 2021): 3}}
    assert grapher_tab.metadata.short_name == "_1"


def test_yield_wide_table_2():
    df = pd.DataFrame(
        {
            "year": [2019, 2020, 2021],
            "entity_id": [1, 2, 3],
            "metric_name": [1, 2, 3],
            "age": [10, 11, 12],
        }
    )
    table = Table(df.set_index(["entity_id", "year", "age"]))
    table.metric_name.metadata.unit = "kg"
    grapher_tab = list(yield_wide_table(table))[0]
    # assert grapher_tab.to_dict() == {"_1": {(1, 2019): 1, (2, 2020): 2, (3, 2021): 3}}
    assert grapher_tab.metadata.short_name == "metric_name__12"

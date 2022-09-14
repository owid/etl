from typing import Iterable

import pandas as pd
from gbd_tools import create_var_name
from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names

N = Names(__file__)
N = Names("/Users/fionaspooner/Documents/OWID/repos/etl/etl/steps/grapher/ihme_gbd/2019/gbd_risk.py")


def get_grapher_dataset() -> catalog.Dataset:
    dataset = N.garden_dataset
    # combine sources into a single one and create proper names
    dataset.metadata = gh.adapt_dataset_metadata_for_grapher(dataset.metadata)
    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["gbd_risk"]
    table = pd.DataFrame(table.reset_index())
    create_var_name(table)
    # convert `country` into `entity_id` and set indexes for `yield_wide_table`
    table = gh.adapt_table_for_grapher(table)

    # optionally set additional dimensions
    # table = table.set_index(["sex", "income_group"], append=True)

    # convert table into grapher format
    # if you data is in long format, use gh.yield_long_table
    yield from gh.yield_wide_table(table, na_action="drop")

from typing import Iterable

from owid import catalog
from owid.catalog import Dataset, VariableMeta

from etl import grapher_helpers as gh
from etl.helpers import Names

from .gbd_tools import create_var_name

N = Names(__file__)
N = Names("/Users/fionaspooner/Documents/OWID/repos/etl/etl/steps/grapher/ihme_gbd/2019/gbd_cause.py")


def get_grapher_dataset() -> catalog.Dataset:
    dataset = N.garden_dataset
    # combine sources into a single one and create proper names
    dataset.metadata = gh.adapt_dataset_metadata_for_grapher(dataset.metadata)
    return dataset


N.garden_dataset.path


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    ds_garden = Dataset(N.garden_dataset.path)
    gbd_tables = ds_garden.table_names
    for table in gbd_tables:
        df = dataset[table]
        df = create_var_name(df)
        df_gr = df.groupby("variable")
        for var_name, df_var in df_gr:
            df_var["meta"] = VariableMeta(
                title=var_name,
                description=ds_garden.metadata.description,
                sources="Institute for Health Metrics and Evaluation - Global Burden of Disease (2019)",
                unit=df_var["unit"].iloc[0],
                # short_unit=df_var["short_unit"].iloc[0],
                additional_info=None,
            )
            df_var = df_var[["country", "year", "value", "variable", "meta"]].copy()
            # convert `country` into `entity_id` and set indexes for `yield_wide_table`
            table = gh.adapt_table_for_grapher(df_var)

            # optionally set additional dimensions
            # table = table.set_index(["sex", "income_group"], append=True)

            # convert table into grapher format
            # if you data is in long format, use gh.yield_long_table
            yield from gh.yield_long_table(table)

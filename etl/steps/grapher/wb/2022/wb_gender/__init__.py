from copy import deepcopy
from structlog import get_logger
from owid import catalog
from etl.paths import DATA_DIR

import etl.grapher_helpers as gh

log = get_logger()

DATASET_GARDEN = DATA_DIR / "garden/wb/2022/wb_gender"


def process_table(table: catalog.Table) -> catalog.Table:
    # Reset index
    table = table.reset_index()
    # Harmonize country names
    table = table.set_index(["country", "year"])
    return table


def add_variable_metadata(table: catalog.Table) -> catalog.Table:
    metadata = deepcopy(table.metadata)
    # Add variable metadata
    variables = table[["variable"]].drop_duplicates()
    variables["meta"] = variables["variable"].astype(str).apply(lambda x: catalog.VariableMeta(title=x, unit=""))
    columns_idx = table.index.names
    table = table.reset_index()
    table = table.merge(variables, on="variable")
    table = table.set_index(columns_idx)
    # Add metadata back
    table.metadata = metadata
    return table


def add_table_to_dataset(dataset: catalog.Dataset, table: catalog.Table) -> None:
    # Add table to dataset
    # dataset.add(table)
    for wide_table in gh.long_to_wide_tables(table):
        # table is generated for every column, use it as a table name
        wide_table.metadata.short_name = wide_table.columns[0]
        dataset.add(catalog.utils.underscore_table(wide_table))


def run(dest_dir: str) -> None:
    # Load dataset from garden
    ds_garden = catalog.Dataset(DATASET_GARDEN)
    # Initiate dataset in grapher
    ds_grapher = catalog.Dataset.create_empty(dest_dir, ds_garden.metadata)
    # Process table
    table = process_table(ds_garden["data"])
    # Add metadata
    table = add_variable_metadata(table)
    # Temporary sources mock
    ds_grapher.sources = [catalog.Source(name="World Bank")]
    # Add table to dataset
    # ds_grapher.add(table)
    add_table_to_dataset(ds_grapher, table)
    # Save dataset
    ds_grapher.save()

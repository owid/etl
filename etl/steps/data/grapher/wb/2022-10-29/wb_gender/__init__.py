from copy import deepcopy

import pandas as pd
from owid import catalog
from structlog import get_logger

import etl.grapher.helpers as gh
from etl.paths import DATA_DIR

log = get_logger()

DATASET_GARDEN = DATA_DIR / "garden/wb/2022-10-29/wb_gender"


def process_table(table: catalog.Table) -> catalog.Table:
    log.info("Processing table...")
    # Set new index
    table = table.reset_index().set_index(["country", "year"])
    return table


def add_variable_metadata(table: catalog.Table, table_metadata: catalog.Table) -> catalog.Table:
    log.info("Adding variable metadata...")

    def _build_variable_metadata_description(table_meta: catalog.Table) -> str:
        description = f"{table_meta['long_definition']}"
        fields_additional = [
            ("topic", "Topic"),
            ("series_code", "Series code"),
            ("source", "Original source"),
        ]
        for field in fields_additional:
            if not pd.isnull(table_meta[field[0]]):
                description += f"\n\n{field[1]}: {table_meta[field[0]]}"
        return description

    metadata = deepcopy(table.metadata)
    # Add variable metadat
    meta = table_metadata.apply(
        lambda variable: catalog.VariableMeta(
            title=variable["indicator_name"],
            description=_build_variable_metadata_description(variable),
            sources=table.metadata.dataset.sources,
            licenses=[catalog.License(name=variable["license_name"], url=variable["license_url"])],
            unit=variable["unit"],
            short_unit=variable["short_unit"],
            display=dict(name=variable["indicator_name"]),
        ),
        axis=1,
    )
    meta.name = "meta"
    meta = pd.concat([table_metadata[["indicator_name"]], meta], axis=1).rename(columns={"indicator_name": "variable"})
    columns_idx = table.index.names
    table = table.reset_index()
    table = table.merge(meta, on="variable")
    table = table.set_index(columns_idx)
    # Add metadata back
    table.metadata = metadata
    return table


def add_table_to_dataset(dataset: catalog.Dataset, table: catalog.Table) -> None:
    # Add table to dataset
    # dataset.add(table)
    log.info("Adding table to dataset...")
    table["variable"] = table["variable"].map(catalog.utils.underscore)
    for wide_table in gh.long_to_wide_tables(table):
        dataset.add(wide_table)


def run(dest_dir: str) -> None:
    # Load dataset from garden
    ds_garden = catalog.Dataset(DATASET_GARDEN)
    # Initiate dataset in grapher
    ds_grapher = catalog.Dataset.create_empty(dest_dir, ds_garden.metadata)
    # Process table
    table = process_table(ds_garden["wb_gender"])
    # Add metadata
    table_metadata = ds_garden["metadata_variables"]
    table = add_variable_metadata(table, table_metadata)
    # Temporary sources mock
    ds_grapher.sources = ds_garden.metadata.sources
    # Add table to dataset
    # ds_grapher.add(table)
    add_table_to_dataset(ds_grapher, table)
    # Save dataset
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def copy_variable_metadata(tb: Table, tables: list[Table]) -> Table:
    """Restore variable metadata after merging tables.

    owid.catalog.processing.merge preserves most metadata, but currently drops
    description_processing. Copying the original variable metadata keeps the garden
    metadata intact in the combined Grapher table.
    """
    for table in tables:
        columns = [column for column in table.columns if column in tb.columns]
        tb_with_metadata = tb[columns].copy_metadata(table[columns])
        for column in columns:
            tb[column].metadata = tb_with_metadata[column].metadata.copy()
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its tables.
    ds_garden = paths.load_dataset("eggs_and_hens_statistics")
    tb_eggs = ds_garden.read("egg_statistics", reset_index=True)
    tb_hens = ds_garden.read("hen_statistics", reset_index=True)

    #
    # Process data.
    #
    # Combine the two garden tables for Grapher, preserving the existing catalog paths used by explorers.
    tb = pr.merge(tb_hens, tb_eggs, on=["country", "year"], how="outer", validate="one_to_one")
    tb = copy_variable_metadata(tb, tables=[tb_hens, tb_eggs])
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save new grapher dataset.
    ds_grapher.save()

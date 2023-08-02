"""Load a garden dataset and create a grapher dataset."""

from typing import cast

from owid import catalog
from owid.catalog import Dataset

from etl.helpers import PathFinder, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("mortality_database"))

    # Read table names from garden dataset.
    table_names = sorted(ds_garden.table_names)

    ds_grapher = catalog.Dataset.create_empty(dest_dir, ds_garden.metadata)
    # terate through each table and add them to grapher
    for table in table_names:
        tab = ds_garden[table]
        ds_grapher.add(tab)
    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()

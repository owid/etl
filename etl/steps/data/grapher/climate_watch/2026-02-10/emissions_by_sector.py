"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read all tables.
    ds_garden = paths.load_dataset("emissions_by_sector")
    tables = [ds_garden.read(table_name, reset_index=False) for table_name in ds_garden.table_names]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(default_metadata=ds_garden.metadata, tables=tables)
    ds_grapher.save()

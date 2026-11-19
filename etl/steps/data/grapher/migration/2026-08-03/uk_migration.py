"""Load the garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("uk_migration")
    tables = [ds_garden.read(name, reset_index=False) for name in ds_garden.table_names]

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)
    ds_grapher.save()

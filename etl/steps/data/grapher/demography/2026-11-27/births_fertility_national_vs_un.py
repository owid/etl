"""Load the garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load garden dataset and read its tables.
    ds_garden = paths.load_dataset("births_fertility_national_vs_un")
    tables = [ds_garden.read(name, reset_index=False) for name in ds_garden.table_names]

    # Save outputs.
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)
    ds_grapher.save()

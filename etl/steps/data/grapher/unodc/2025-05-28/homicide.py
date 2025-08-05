"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("homicide")

    # Read table from garden dataset.
    table_names = ds_garden.table_names
    # if your data is in long format, you can use `grapher.helpers.long_to_wide_tables`
    # to get into wide format
    tables = []
    for table_name in table_names:
        table = paths.garden_dataset[table_name]
        tables.append(table)
    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

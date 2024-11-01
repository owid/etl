"""Load a garden dataset and create a grapher dataset."""
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("unodc")

    table_names = ds_garden.table_names
    # if your data is in long format, you can use `grapher_helpers.long_to_wide_tables`
    # to get into wide format
    tables = []
    for table_name in table_names:
        table = paths.garden_dataset[table_name]
        tables.append(table)

    ds_grapher = create_dataset(dest_dir, tables=tables, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Garden tables that are not published in grapher.
TABLES_NOT_IN_GRAPHER = ["total_oda"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("climate_related_official_development_assistance")

    # Read tables from garden dataset. Total ODA is only used by the static chart of climate-related bilateral aid.
    tables = [
        ds_garden.read(table_name, reset_index=False)
        for table_name in ds_garden.table_names
        if table_name not in TABLES_NOT_IN_GRAPHER
    ]

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("income_groups")

    # Read table of income groups (a dynamic classification that changes over the years).
    tb_dynamic = ds_garden.read("income_groups")

    # Read table of latest income groups (a static classification).
    tb_static = ds_garden.read("income_groups_latest")

    #
    # Process data.
    #
    # Prepare static table.
    tb_static = tb_static.rename(columns={"classification": "classification_latest"}, errors="raise")

    # Add a year column (with the latest year from the dynamic table).
    tb_static["year"] = tb_dynamic["year"].max()

    # Improve table formats.
    tb_static = tb_static.format()
    tb_dynamic = tb_dynamic.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_static, tb_dynamic], default_metadata=ds_garden.metadata)
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("trade")

    # Read table from garden dataset.
    tb = ds_garden.read("trade", reset_index=False)
    tb["import_rank"] = tb["import_rank"].astype("string")
    tb["china_imports_share_of_gdp"] = tb["china_imports_share_of_gdp"].astype("string")

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP version
# NOTE: Change this in case of new PPP versions in the future
# TODO: Change to 2021 prices
PPP_VERSION_CURRENT = 2017


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("world_bank_pip")

    # Read tables from garden dataset.
    tb = ds_garden.read(f"income_consumption_{PPP_VERSION_CURRENT}", reset_index=False)

    #
    # Process data.
    #
    # Drop reporting_level and welfare_type columns
    tb = tb.drop(columns=["reporting_level", "welfare_type"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()

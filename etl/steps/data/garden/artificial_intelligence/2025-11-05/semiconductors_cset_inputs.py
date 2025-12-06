"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("semiconductors_cset_inputs")

    # Read table from meadow dataset.
    tb = ds_meadow.read("semiconductors_cset_inputs")

    #
    # Process data.
    #
    tb["market_size_value"] = tb["market_size_value"] * 1e9  # Convert from billions to units
    tb = tb.drop(columns=["description"])  # Drop description column

    # Set index
    tb = tb.format(["input_name", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

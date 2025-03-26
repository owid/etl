"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Unit conversion factor to change from bushel of corn to metric tonnes.
BUSHELS_OF_CORN_TO_TONNES = 0.0254

# Unit conversion factor to change from acres to hectares.
ACRES_TO_HECTARES = 0.4047


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("us_corn_yields")
    tb = ds_meadow["us_corn_yields"]

    #
    # Process data.
    #
    # Change units of corn yield.
    tb["corn_yield"] *= BUSHELS_OF_CORN_TO_TONNES / ACRES_TO_HECTARES

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

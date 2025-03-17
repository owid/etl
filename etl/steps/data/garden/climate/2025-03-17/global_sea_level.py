"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("global_sea_level")
    tb = ds_meadow["global_sea_level"].reset_index()

    #
    # Process data.
    #
    # Add column with average values between Church & White and UHSLC.
    tb["sea_level_average"] = tb[["sea_level_church_and_white_2011", "sea_level_uhslc"]].mean(axis=1)
    tb["sea_level_average"] = tb["sea_level_average"].copy_metadata(tb["sea_level_church_and_white_2011"])

    # Add location column.
    tb["location"] = "World"

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["location", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_garden.save()

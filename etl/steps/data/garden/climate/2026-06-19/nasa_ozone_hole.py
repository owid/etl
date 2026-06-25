"""Load a meadow dataset and create a garden dataset.

This step adds a new column: "country" with the value "World", since the data in this dataset
is only for the world."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("nasa_ozone_hole")

    # Read table from meadow dataset.
    tb = ds_meadow.read("nasa_ozone_hole")

    # Add country column (only one entity: "World")
    tb.loc[:, "country"] = "World"

    # Format
    tb = tb.format(["year", "country"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

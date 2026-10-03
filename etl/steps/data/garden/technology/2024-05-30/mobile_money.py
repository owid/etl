"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("mobile_money")

    # Read table from meadow dataset.
    tb = ds_meadow["mobile_money"].reset_index()

    # Count values available per year.
    tally = tb.groupby("year").size()
    # Filter data to keep only years with all regions.
    first_full_year = tally[tally == 6].index.min()
    tb = tb[tb.year >= first_full_year]

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

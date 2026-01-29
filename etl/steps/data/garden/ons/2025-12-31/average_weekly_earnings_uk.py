"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("average_weekly_earnings_uk")

    # Read table from meadow dataset.
    tb = ds_meadow.read("average_weekly_earnings_uk")

    #
    # Process data.
    #
    # Make date column datetime
    tb["date"] = tb["date"].astype("datetime64[ns]")

    # From date column, extract year to calculate yearly average
    tb["year"] = tb["date"].dt.year

    # Group by year and calculate average weekly earnings per year
    tb = tb.groupby("year", as_index=False)["average_weekly_earnings"].mean()

    # Add country column
    tb["country"] = "United Kingdom"

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

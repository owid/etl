"""Load a garden dataset and create a grapher dataset."""

from etl.grapher.helpers import adapt_table_with_dates_to_grapher
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its monthly table.
    ds_garden = paths.load_dataset("climate_change_impacts")
    tb = ds_garden.read("climate_change_impacts_monthly")

    #
    # Process data.
    #
    # Create a country column (required by grapher).
    tb = tb.rename(columns={"location": "country"}, errors="raise")

    # Adapt table with dates to grapher requirements.
    tb = adapt_table_with_dates_to_grapher(tb, time_interval="month")

    # Ocean pH measurements are irregular Aloha-station cruise dates (17-31 day gaps), not monthly
    # aggregates. With timeInterval "month", grapher snaps them to the 1st of the month, and the
    # ~15 months holding two cruises collapse onto one time key (values silently dropped).
    for column in ["ocean_ph", "ocean_ph_yearly_average"]:
        tb[column].metadata.display["timeInterval"] = "day"

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()

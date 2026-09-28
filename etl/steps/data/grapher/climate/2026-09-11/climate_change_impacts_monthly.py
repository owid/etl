"""Load a garden dataset and create a grapher dataset."""

from etl.grapher.helpers import adapt_table_with_dates_to_grapher
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Most columns in this table are monthly, reported on the 15th of each month, so the whole table is
# adapted with time_interval="month". These columns are not, and need their own interval: grapher
# snaps sub-yearly times to the start of their period, so a column tagged with a coarser interval
# than its dates actually have collapses several points onto one time key and silently keeps only
# the first of them.
TIME_INTERVAL_OVERRIDES = {
    # Ocean pH is measured on Aloha-station cruise dates, with 17-31 day gaps.
    "ocean_ph": "day",
    "ocean_ph_yearly_average": "day",
    # IMBIE reports drifting fractional-year stamps (1992-01-01, 1992-01-31, 1992-03-01, ...), so
    # some months hold two measurements and others none.
    "cumulative_ice_mass_change_imbie": "day",
    # NASA's ice mass follows GRACE satellite overpasses, on varying days of the month.
    "land_ice_mass_nasa": "day",
    # Sea level is quarterly, reported on the 15th of the quarter's first month.
    "sea_level_church_and_white_2011": "quarter",
    "sea_level_uhslc": "quarter",
    "sea_level_average": "quarter",
}


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

    # Give the columns whose dates are not monthly their own time interval.
    for column, time_interval in TIME_INTERVAL_OVERRIDES.items():
        tb[column].metadata.display["timeInterval"] = time_interval

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()

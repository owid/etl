"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

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

# Pandas period alias for each interval whose points must be unique within a period.
PERIOD_ALIAS_BY_INTERVAL = {"month": "M", "quarter": "Q"}


def sanity_check_time_intervals(tb: Table) -> None:
    """Check that no column holds two points within one period of its declared time interval.

    Such a pair would be indistinguishable to grapher, which would plot only the first of the two.
    A new or updated indicator whose dates are irregular needs an entry in TIME_INTERVAL_OVERRIDES.
    """
    dates = pr.to_datetime(tb["date"].astype(str))
    for column in tb.drop(columns=["country", "date"]).columns:
        alias = PERIOD_ALIAS_BY_INTERVAL.get(TIME_INTERVAL_OVERRIDES.get(column, "month"))
        if alias is None:
            # The column is tagged "day"; its dates are already as precise as grapher can plot.
            continue
        periods = tb.loc[tb[column].notna(), ["country"]].assign(period=dates.dt.to_period(alias))
        collisions = len(periods) - len(periods.drop_duplicates())
        assert collisions == 0, (
            f"Column `{column}` has {collisions} points sharing a period with another point, which "
            f"grapher would drop. Add it to TIME_INTERVAL_OVERRIDES with the interval its dates "
            f"actually represent."
        )


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

    # Check that every column's declared time interval matches the precision of its dates.
    sanity_check_time_intervals(tb)

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

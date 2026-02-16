from owid.catalog import Table

from etl.helpers import PathFinder
from etl.steps.data.garden.covid.latest.shared import add_last12m_to_metric

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Keep data only until 2023, as the baseline doesn't make sense beyond that
YEAR_LIMIT = 2023


def run() -> None:
    # Load table
    ds_meadow = paths.load_dataset()
    tb = ds_meadow.read("excess_mortality_economist")

    # Set type
    tb = tb.astype({"country": "string", "date": "datetime64[ns]"})

    # Harmonize country names
    tb = paths.regions.harmonize_names(tb)

    # Add last 12 months
    tb = add_last12m_values(tb)
    # Drop unused column
    tb = tb.drop(columns=["known_excess_deaths"])

    # Keep data until 2023
    tb = tb.loc[tb["date"].dt.year <= YEAR_LIMIT]

    # Set index
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(
        tables=[tb],
        default_metadata=ds_meadow.metadata,
        formats=["csv", "feather"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_last12m_values(tb: Table) -> Table:
    """Add last 12 month data."""
    columns = [
        "cumulative_estimated_daily_excess_deaths",
        "cumulative_estimated_daily_excess_deaths_ci_95_bot",
        "cumulative_estimated_daily_excess_deaths_ci_95_top",
    ]
    for col in columns:
        tb = add_last12m_to_metric(tb, col)
        tb = add_last12m_to_metric(tb, f"{col}_per_100k")
    return tb

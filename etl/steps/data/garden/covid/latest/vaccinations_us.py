"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from shared import make_monotonic

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccinations_us")

    # Read table from meadow dataset.
    tb = ds_meadow["vaccinations_us"].reset_index()

    #
    # Process data.
    #
    # Dtype date
    tb["date"] = pd.to_datetime(tb["date"])
    # Add total boosters
    tb = add_total_boosters(tb)
    # Something else
    tb = tb.groupby("state", observed=True).apply(make_monotonic, max_removed_rows=None).reset_index(drop=True)  # type: ignore
    # Add per-capita
    tb = add_per_capita(tb)
    # Add smoothed indicators
    tb = add_smoothed(tb)
    # Add usage
    tb["share_doses_used"] = tb["total_vaccinations"].div(tb["total_distributed"]).round(3)
    # Drop census column
    tb = tb.drop(columns=["census_2019"])
    # Select columns
    tb = tb[
        [
            "date",
            "state",
            "total_vaccinations",
            "total_distributed",
            "people_vaccinated",
            "people_fully_vaccinated_per_hundred",
            "total_vaccinations_per_hundred",
            "people_fully_vaccinated",
            "people_vaccinated_per_hundred",
            "distributed_per_hundred",
            "daily_vaccinations_raw",
            "daily_vaccinations",
            "daily_vaccinations_per_million",
            "share_doses_used",
            "total_boosters",
            "total_boosters_per_hundred",
        ]
    ]

    # Format
    tb = tb.format(["state", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_total_boosters(tb: Table) -> Table:
    """Add counts of total booster doses."""
    # Assuming df is your DataFrame
    tb = tb.sort_values(by=["state", "date"])

    # Define a function to calculate total_boosters for each group
    def _calculate_total_boosters(tb):
        tb["total_boosters"] = (
            tb["total_vaccinations"]
            - tb["people_vaccinated"]
            - tb["people_fully_vaccinated"]
            + tb["single_shots"].ffill()
        )
        return tb

    # Apply the function to each group
    tb = tb.groupby("state").apply(_calculate_total_boosters).reset_index(drop=True)  # type: ignore

    tb.loc[tb["date"] < "2021-08-27", "total_boosters"] = pd.NA
    tb.loc[tb["total_boosters"] < 0, "total_boosters"] = pd.NA
    return tb


def add_per_capita(tb: Table) -> Table:
    """Add per capita columns."""
    cols = [
        "people_fully_vaccinated",
        "total_vaccinations",
        "people_vaccinated",
        "total_distributed",
        "total_boosters",
    ]
    for col in cols:
        tb[col + "_per_hundred"] = tb[col].div(tb["census_2019"]).mul(100)
    tb = tb.rename(
        columns={
            "total_distributed_per_hundred": "distributed_per_hundred",
        }
    )
    for var in tb.columns:
        if "_per_hundred" in var:
            tb.loc[tb[var].notnull(), var] = tb.loc[tb[var].notnull(), var].astype(float).round(2)
    return tb


def add_smoothed(tb: Table) -> Table:
    """Add smoothed indicators."""
    tb = tb.sort_values(["date", "state"])
    tb["date"] = pd.to_datetime(tb["date"])
    tb = tb.groupby("state", as_index=False).apply(_smooth_state)  # type: ignore

    # Add metadata
    cols = [
        "daily_vaccinations",
        "daily_vaccinations_per_million",
        "daily_vaccinations_raw",
    ]
    for col in cols:
        tb[col] = tb[col].copy_metadata(tb["total_vaccinations"])
    return tb


def _smooth_state(tb: Table) -> Table:
    tb = tb.set_index("date").resample("1D").asfreq().reset_index().sort_values("date")
    tb[["state", "census_2019"]] = tb[["state", "census_2019"]].ffill()
    interpolated_totals = tb["total_vaccinations"].interpolate("linear")
    tb["daily_vaccinations"] = (
        (interpolated_totals - interpolated_totals.shift(1)).rolling(7, min_periods=1).mean().round()  # type: ignore
    )
    tb["daily_vaccinations_raw"] = (tb["total_vaccinations"] - tb["total_vaccinations"].shift(1)).copy_metadata(
        tb["total_vaccinations"]
    )
    tb["daily_vaccinations_per_million"] = (
        tb["daily_vaccinations"].mul(1_000_000).div(tb["census_2019"]).round().copy_metadata(tb["total_vaccinations"])
    )
    return tb

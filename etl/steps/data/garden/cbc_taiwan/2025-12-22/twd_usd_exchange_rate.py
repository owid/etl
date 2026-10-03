"""Load meadow dataset and create garden dataset with monthly and annual averages."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create garden dataset with daily, monthly, and annual exchange rates."""
    # Load inputs.
    ds_meadow = paths.load_dataset("twd_usd_exchange_rate")

    # Load the daily data
    tb_daily = ds_meadow.read("twd_usd_exchange_rate")

    # Create monthly averages
    tb_daily["year"] = tb_daily["date"].dt.year
    tb_daily["month"] = tb_daily["date"].dt.month

    # Calculate monthly average exchange rate
    tb_monthly = (
        tb_daily.groupby(["year", "month"], as_index=False)["exchange_rate"]
        .mean()
        .rename(columns={"exchange_rate": "exchange_rate_monthly_avg"})
    )

    # Create a date column for the first day of each month
    tb_monthly["date"] = pd.to_datetime(tb_monthly[["year", "month"]].assign(day=1))
    tb_monthly = tb_monthly.drop(columns=["year", "month"])

    # Calculate annual average exchange rate
    tb_annual = (
        tb_daily.groupby("year", as_index=False)["exchange_rate"]
        .mean()
        .rename(columns={"exchange_rate": "exchange_rate_annual_avg"})
    )

    # Format tables
    tb_monthly = tb_monthly.format(["date"], short_name="twd_usd_monthly")
    tb_annual = tb_annual.format(["year"], short_name="twd_usd_annual")

    # Save outputs.
    ds_garden = paths.create_dataset(
        tables=[tb_monthly, tb_annual],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()

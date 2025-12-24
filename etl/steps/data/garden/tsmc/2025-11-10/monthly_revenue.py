"""Load meadow dataset and create garden dataset with enhanced indicators."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create garden dataset with additional calculated indicators."""
    # Load inputs.
    ds_meadow = paths.load_dataset("monthly_revenue")
    ds_exchange = paths.load_dataset("twd_usd_exchange_rate", namespace="cbc_taiwan")

    # Load the TSMC revenue data
    tb_monthly = ds_meadow.read("tsmc_monthly_revenue", reset_index=False)
    tb_yearly = ds_meadow.read("tsmc_yearly_revenue", reset_index=False)

    # Load the exchange rate data
    tb_exchange_monthly = ds_exchange.read("twd_usd_monthly", reset_index=False)
    tb_exchange_annual = ds_exchange.read("twd_usd_annual", reset_index=False)

    # Merge monthly revenue with monthly average exchange rates
    tb_monthly = tb_monthly.reset_index()
    tb_monthly_merged = pd.merge(
        tb_monthly, tb_exchange_monthly, on="date", how="left", validate="one_to_one"
    )

    # Store original revenue metadata
    revenue_origins = tb_monthly_merged["revenue"].metadata.origins

    # Convert monthly revenue from TWD to USD
    tb_monthly_merged["revenue_usd"] = (
        tb_monthly_merged["revenue"] / tb_monthly_merged["exchange_rate_monthly_avg"]
    )

    # Drop TWD revenue and exchange rate columns, keep only USD
    tb_monthly_merged = tb_monthly_merged.drop(columns=["revenue", "exchange_rate_monthly_avg"])

    # Add metadata origins for USD revenue
    tb_monthly_merged["revenue_usd"].metadata.origins = (
        revenue_origins + tb_exchange_monthly["exchange_rate_monthly_avg"].metadata.origins
    )

    # Merge yearly revenue with annual average exchange rates
    tb_yearly = tb_yearly.reset_index()
    tb_yearly_merged = pd.merge(tb_yearly, tb_exchange_annual, on="year", how="left", validate="one_to_one")

    # Store original revenue metadata
    revenue_origins_yearly = tb_yearly_merged["revenue"].metadata.origins

    # Convert yearly revenue from TWD to USD
    tb_yearly_merged["revenue_usd"] = tb_yearly_merged["revenue"] / tb_yearly_merged["exchange_rate_annual_avg"]

    # Drop TWD revenue and exchange rate columns, keep only USD
    tb_yearly_merged = tb_yearly_merged.drop(columns=["revenue", "exchange_rate_annual_avg"])

    # Add metadata origins for USD revenue
    tb_yearly_merged["revenue_usd"].metadata.origins = (
        revenue_origins_yearly + tb_exchange_annual["exchange_rate_annual_avg"].metadata.origins
    )

    # Format tables
    tb_monthly_merged = tb_monthly_merged.format(["date"], short_name="tsmc_monthly_revenue")
    tb_yearly_merged = tb_yearly_merged.format(["year"], short_name="tsmc_yearly_revenue")

    # Save outputs.
    ds_garden = paths.create_dataset(
        tables=[tb_monthly_merged, tb_yearly_merged],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()

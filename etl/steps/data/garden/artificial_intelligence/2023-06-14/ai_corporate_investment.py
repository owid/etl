"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("ai_corporate_investment.start")

    #
    # Load inputs.
    #
    # Load AI corporate investment snapshot
    snap = cast(Snapshot, paths.load_dependency("ai_corporate_investment.csv"))
    df = pd.read_csv(snap.path)

    df.rename(columns={"Label": "type", "Year": "year"}, inplace=True)
    # convert to regulat number
    df["Total Investment (in Billions of U.S. Dollars)"] = df["Total Investment (in Billions of U.S. Dollars)"] * 10e8
    df.rename(
        columns={"Label": "type", "Year": "year", "Total Investment (in Billions of U.S. Dollars)": "Total Investment"},
        inplace=True,
    )
    # Add Total Investment
    total_investment = df.groupby("year")["Total Investment"].sum()
    # Create a DataFrame from the total investment series
    total_df = pd.DataFrame(
        {"year": total_investment.index, "Total Investment": total_investment.values, "type": "Total"}
    )

    # Merge the total investment DataFrame with the original DataFrame
    df = pd.merge(df, total_df, on=["year", "type", "Total Investment"], how="outer")

    # Import US CPI data from the API
    snap = cast(Snapshot, paths.load_dependency("us_cpi.csv"))

    # Now read the file with pandas
    df_wdi_cpi_us = pd.read_csv(snap.path)
    if df_wdi_cpi_us is None:
        log.info("Failed to import US CPI data from the API.")
        return

    # Adjust CPI values so that 2021 is the reference year (2021 = 100)
    cpi_2021 = df_wdi_cpi_us.loc[df_wdi_cpi_us["year"] == 2021, "fp_cpi_totl"].values[0]
    df_wdi_cpi_us["cpi_adj_2021"] = 100 * df_wdi_cpi_us["fp_cpi_totl"] / cpi_2021

    # Assuming df5 exists and it has a column named 'Year'
    # Merge df5 with CPI data
    df_cpi_inv = pd.merge(df_wdi_cpi_us, df, on="year", how="inner")

    df_cpi_inv["total_corporate_investment_by_activity_inflation_adjusted"] = round(
        100 * df_cpi_inv["Total Investment"] / df_cpi_inv["cpi_adj_2021"]
    )
    df_cpi_inv.drop(["cpi_adj_2021", "fp_cpi_totl"], axis=1, inplace=True)

    tb = Table(df_cpi_inv, short_name="ai_corporate_investment", underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_corporate_investment.end")

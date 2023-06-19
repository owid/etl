"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
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

    # Load WDI
    ds_wdi = cast(Dataset, paths.load_dependency("wdi"))
    tb_wdi = ds_wdi["wdi"]

    # Assume country and year are multi-index
    df_wdi_cpi = tb_wdi[["fp_cpi_totl"]]

    # Select only the data for the "United States"
    df_wdi_cpi = df_wdi_cpi.sort_index()
    df_wdi_cpi_us = df_wdi_cpi.loc[("United States",)]

    # Adjust CPI values so that 2021 is the reference year (2021 = 100)
    cpi_2021 = df_wdi_cpi_us.loc[(2021,), "fp_cpi_totl"]

    # Adjust 'fp_cpi_totl' column by the 2021 CPI
    df_wdi_cpi_us["cpi_adj_2021"] = 100 * df_wdi_cpi_us["fp_cpi_totl"] / cpi_2021
    df_wdi_cpi_us.reset_index(inplace=True)

    # Assuming df5 exists and it has a column named 'Year'
    # Merge df5 with CPI data
    df_cpi_inv = pd.merge(df_wdi_cpi_us, df, on="year", how="left")

    df_cpi_inv["total_corporate_investment_by_activity_inflation_adjusted"] = round(
        100 * df_cpi_inv["Total Investment"] / df_cpi_inv["cpi_adj_2021"]
    )

    tb = Table(df_cpi_inv, short_name="ai_corporate_investment", underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_corporate_investment.end")

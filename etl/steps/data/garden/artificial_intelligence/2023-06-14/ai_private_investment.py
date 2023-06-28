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
    log.info("ai_private_investment.start")
    #
    # Load inputs.
    #
    # Load AI corporate investment snapshot
    snap = cast(Snapshot, paths.load_dependency("ai_private_investment.csv"))
    print(snap.path)
    df = pd.read_csv(snap.path)

    exclude_columns = ["Year", "Geographic Area"]
    df.loc[:, ~df.columns.isin(exclude_columns)] *= 1e9
    df["Total"] = df.loc[:, ~df.columns.isin(exclude_columns)].sum(axis=1)
    # Calculate the yearly sum countries
    yearly_sum = df.groupby("Year")[df.columns[2:]].sum().reset_index()
    yearly_sum["Geographic Area"] = "Total"
    # Add the yearly sum rows to the DataFrame
    df = pd.concat([df, yearly_sum], ignore_index=True)
    df.rename(columns={"Year": "year"}, inplace=True)

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
    df_wdi_cpi_us.drop("fp_cpi_totl", axis=1, inplace=True)
    df_cpi_inv = pd.merge(df_wdi_cpi_us, df, on="year", how="inner")

    exclude_for_inflation = ["Year", "Geographic Area", "cpi_adj_2021"]
    for col in df_cpi_inv.columns:
        if col not in exclude_for_inflation:
            df_cpi_inv[col] = round(100 * df_cpi_inv[col] / df_cpi_inv["cpi_adj_2021"])

    tb = Table(df_cpi_inv, short_name=paths.short_name, underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_private_investment.end")

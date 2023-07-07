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
    cols_to_adjust_for_infaltion = [
        "Data Management, Processing, Cloud",
        "Medical and Healthcare",
        "Fintech",
        "AV",
        "Semiconductor",
        "Industrial Automation, Network",
        "Retail",
        "Fitness and Wellness",
        "NLP, Customer Support",
        "Energy, Oil, and Gas",
        "Cybersecurity, Data Protection",
        "Drones",
        "Marketing, Digital Ads",
        "HR Tech",
        "Facial Recognition",
        "Insurtech",
        "Agritech",
        "Sales Enablement",
        "AR/VR",
        "Ed Tech",
        "Geospatial",
        "Legal Tech",
        "Entertainment",
        "Music, Video Content",
        "VC",
        "Total",
        "Total (focus area)",
    ]

    log.info("ai_private_investment.start")
    #
    # Load inputs.
    #
    # Load AI corporate investment snapshot
    snap = cast(Snapshot, paths.load_dependency("ai_private_investment.csv"))
    df = pd.read_csv(snap.path)
    df["Geographic Area"] = df["Geographic Area"].replace(
        {"CN": "China", "US": "United States", "EU/UK": "European Union and United Kingdom", "World": "World"}
    )

    snap_total = cast(Snapshot, paths.load_dependency("ai_private_investment_total.csv"))
    df_total = pd.read_csv(snap_total.path)

    df_total.rename(
        columns={"Total Investment (in Billions of U.S. Dollars)": "Total", "Label": "Geographic Area"}, inplace=True
    )
    df = pd.merge(df, df_total, on=["Year", "Geographic Area"], how="outer")

    df.rename(columns={"Year": "year"}, inplace=True)
    df.loc[:, df.columns.isin(cols_to_adjust_for_infaltion)] *= 1e9

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

    for col in df_cpi_inv[cols_to_adjust_for_infaltion]:
        df_cpi_inv[col] = round(100 * df_cpi_inv[col] / df_cpi_inv["cpi_adj_2021"])

    df_cpi_inv.drop("cpi_adj_2021", axis=1, inplace=True)

    df_cpi_inv.rename(columns={"Geographic Area": "country"}, inplace=True)
    df_cpi_inv["country"] = df_cpi_inv["country"].fillna("World")

    tb = Table(df_cpi_inv, short_name=paths.short_name, underscore=True)
    tb.set_index(["year", "country", "focus_area"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_private_investment.end")

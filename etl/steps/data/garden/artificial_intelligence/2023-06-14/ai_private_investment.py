"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
import us_cpi
from owid.catalog import Table
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

    df_wdi_cpi_us = us_cpi.import_US_cpi_API()

    # Adjust CPI values so that 2021 is the reference year (2021 = 100)
    cpi_2021 = df_wdi_cpi_us[df_wdi_cpi_us["year"] == 2021]["fp_cpi_totl"].values[0]
    # Adjust 'fp_cpi_totl' column by the 2021 CPI
    df_wdi_cpi_us["cpi_adj_2021"] = 100 * df_wdi_cpi_us["fp_cpi_totl"] / cpi_2021

    df_wdi_cpi_us.drop("fp_cpi_totl", axis=1, inplace=True)
    df_cpi_inv = pd.merge(df_wdi_cpi_us, df, on="year", how="inner")

    for col in df_cpi_inv[cols_to_adjust_for_infaltion]:
        df_cpi_inv[col] = round(100 * df_cpi_inv[col] / df_cpi_inv["cpi_adj_2021"])

    df_cpi_inv.drop("cpi_adj_2021", axis=1, inplace=True)

    df_cpi_inv.rename(columns={"Geographic Area": "country"}, inplace=True)
    df_cpi_inv["country"] = df_cpi_inv["country"].fillna("World")
    merged_df = reshape_for_plotting(df_cpi_inv)

    tb = Table(merged_df, short_name=paths.short_name, underscore=True)
    tb.set_index(["year", "focus_area"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_private_investment.end")


def reshape_for_plotting(df):
    """
    Reshapes the input DataFrame for plotting purposes.

    Args:
        df (pandas.DataFrame): The input DataFrame containing the data to be reshaped.

    Returns:
        pandas.DataFrame: The reshaped DataFrame suitable for plotting.
    """

    # Mapping dictionary for renaming focus areas
    name_map_dict = {
        "Data Management, Processing, Cloud": "Data management",
        "Medical and Healthcare": "Medical and healthcare",
        "Fintech": "Financial technology",
        "Retail": "Retail",
        "Cybersecurity, Data Protection": "Cybersecurity",
        "Sales Enablement": "Sales enablement",
        "Entertainment": "Entertainment",
        "Insurtech": "Insurance technology",
        "AV": "AI ventures",
        "Industrial Automation, Network": "Industrial automation",
        "Music, Video Content": "Music and video content",
        "Energy, Oil, and Gas": "Energy, oil and gas",
        "Ed Tech": "Educational technology",
        "NLP, Customer Support": "Natural Language Processing, customer support",
        "Marketing, Digital Ads": "Marketing and digital ads",
        "Agritech": "Agricultural technology",
        "Geospatial": "Geospatial",
        "Fitness and Wellness": "Fitness and wellness",
        "AR/VR": "Augmented or virtual reality",
        "Semiconductor": "Semiconductors",
        "Drones": "Drones",
        "HR Tech": "Human Resources technology",
        "Facial Recognition": "Facial recognition",
        "Legal Tech": "Legal technology",
        "VC": "Venture capital",
    }

    # Select relevant columns for the world data
    cols_world = ["year", "country", "Total (focus area)", "Focus Area"]
    world_df = df[cols_world]

    # Drop rows with missing values in 'Total (focus area)' or 'Focus Area'
    world_df = world_df.dropna(subset=["Total (focus area)", "Focus Area"], how="all")

    # Remove the 'country' column
    world_df.drop("country", axis=1, inplace=True)

    # Reset the index of the DataFrame
    world_df.reset_index(drop=True, inplace=True)

    # Rename the 'Total (focus area)' column to 'World'
    world_df.rename(columns={"Total (focus area)": "World"}, inplace=True)

    # Replace focus area names using the mapping dictionary
    world_df["Focus Area"] = world_df["Focus Area"].replace(name_map_dict)

    # Select columns that are not 'Total (focus area)' or 'Focus Area'
    mask_focus_area = ~df.columns.isin(["Total (focus area)", "Focus Area"])
    focus_area_df = df.loc[:, mask_focus_area]

    # Select columns to check for missing values
    cols_to_check = focus_area_df.columns.difference(["year", "country"])

    # Drop rows with missing values in the selected columns
    df_focus_clean = focus_area_df.dropna(subset=cols_to_check, how="all")

    # Reshape the DataFrame by melting it
    df_melted_focus = df_focus_clean.melt(id_vars=["year", "country"], var_name="Focus Area", value_name="value")

    # Pivot the melted DataFrame to create country-specific columns
    df_pivoted_focus = df_melted_focus.pivot(index=["year", "Focus Area"], columns="country", values="value")

    # Clean up column names by removing whitespace
    df_pivoted_focus.columns = ["".join(col).strip() for col in df_pivoted_focus.columns.values]

    # Reset the index of the pivoted DataFrame
    df_pivoted_focus = df_pivoted_focus.reset_index()

    # Replace focus area names using the mapping dictionary
    df_pivoted_focus["Focus Area"] = df_pivoted_focus["Focus Area"].replace(name_map_dict)

    # Merge the pivoted DataFrame with the world DataFrame
    df_pivoted_focus = df_pivoted_focus.merge(
        world_df[["year", "Focus Area", "World"]], on=["year", "Focus Area"], how="left", suffixes=("", "_world")
    )

    # Fill missing values in the 'World' column with corresponding values from 'World_world' column
    df_pivoted_focus["World"] = df_pivoted_focus["World"].fillna(df_pivoted_focus["World_world"])

    # Drop the redundant 'World_world' column
    df_pivoted_focus = df_pivoted_focus.drop("World_world", axis=1)

    return df_pivoted_focus

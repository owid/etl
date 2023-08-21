"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import pandas as pd
import us_cpi
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def add_world(tb: Table, ds_regions: Dataset) -> Table:
    """
    Append aggregated data for 'World' based on specified regions to the given table.

    Parameters:
    - tb (Table): The original table containing country-specific data.
    - ds_regions (Dataset): Not used in the function, but retained for backward compatibility.

    Returns:
    - Table: Updated table with an additional 'World' entry aggregating data from the specified regions.
    """

    # Create a deep copy of the input table to avoid modifying the original data
    tb_with_regions = tb.copy()

    # List of members representing different regions CSET
    members = [
        "Quad (Australia, India, Japan and the US)",
        "Five Eyes (Australia, Canada, New Zealand, UK, and the US)",
        "Global Partnership on Artificial Intelligence",
        "European Union (27)",
        "ASEAN (Association of Southeast Asian Nations)",
        "North America",
        "Europe",
        "Asia Pacific",
        "Africa",
        "Latin America and the Caribbean",
        "Oceania",
        "NATO",
    ]

    # Filter the table to only include rows corresponding to the countries and not regions
    df_regions = tb_with_regions[~tb_with_regions["country"].isin(members)]

    # Reset the index of the filtered data
    df_regions.reset_index(inplace=True, drop=True)

    # Define aggregation rules for each column excluding "country", "year", and "field"
    numeric_cols = [col for col in df_regions.columns if col not in ["country", "year", "field"]]

    # Group the filtered data by "year" and "field" and aggregate the data based on the defined rules
    result = df_regions.groupby(["year", "field"])[numeric_cols].agg(sum_with_nan).reset_index()

    # Assign the aggregated data to a new country named "World"
    result["country"] = "World"

    # Concatenate the aggregated 'World' data with the original table
    tb = pd.concat([tb_with_regions, result])

    # Reset the index of the concatenated table
    tb.reset_index(inplace=True, drop=True)

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("cset"))

    # Read table from meadow dataset.
    tb = ds_meadow["cset"]
    tb.reset_index(inplace=True)

    #
    # Process data.
    #
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add world
    ds_regions: Dataset = paths.load_dependency("regions")
    tb = add_world(tb=tb, ds_regions=ds_regions)

    # List of columns to include for conversion to millions (investment values)
    _investment_cols = [col for col in tb.columns if "investment" in col]
    # Convert all other columns to million
    tb.loc[:, _investment_cols] *= 1e6

    # Import US CPI data from the API (to adjust investment indicators for inflation)
    df_wdi_cpi_us = us_cpi.import_US_cpi_API()
    if df_wdi_cpi_us is None:
        log.info("Failed to import US CPI data from the API.")
        return

    # Adjust CPI values so that 2021 is the reference year (2021 = 100)
    cpi_2021 = df_wdi_cpi_us.loc[df_wdi_cpi_us["year"] == 2021, "fp_cpi_totl"].values[0]
    # Adjust 'fp_cpi_totl' column by the 2021 CPI
    df_wdi_cpi_us["cpi_adj_2021"] = 100 * df_wdi_cpi_us["fp_cpi_totl"] / cpi_2021

    # Drop original CPI
    df_wdi_cpi_us.drop("fp_cpi_totl", axis=1, inplace=True)
    df_cpi_inv = pd.merge(df_wdi_cpi_us, tb, on="year", how="inner")

    # Updating the investment columns with inflation adjusted values
    for col in df_cpi_inv[_investment_cols]:
        df_cpi_inv[col] = round(100 * df_cpi_inv[col] / df_cpi_inv["cpi_adj_2021"])

    df_cpi_inv.drop("cpi_adj_2021", axis=1, inplace=True)

    # Load population and merge with CSET dataset
    ds_population = cast(Dataset, paths.load_dependency("population"))
    tb_population = ds_population["population"].reset_index(drop=False)
    df_pop_add = pd.merge(
        df_cpi_inv, tb_population[["country", "year", "population"]], how="left", on=["country", "year"]
    )
    # Add per 1 million patents and publications
    df_pop_add["num_patent_applications_per_mil"] = df_pop_add["num_patent_applications"] / (
        df_pop_add["population"] / 1e6
    ).astype("float64")
    df_pop_add["num_patent_granted_per_mil"] = (
        df_pop_add["num_patent_granted"] / (df_pop_add["population"] / 1e6)
    ).astype("float64")
    df_pop_add["num_articles_per_mil"] = (df_pop_add["num_articles"] / (df_pop_add["population"] / 1e6)).astype(
        "float64"
    )
    df_pop_add = df_pop_add.drop("population", axis=1)
    tb = Table(df_pop_add, short_name=paths.short_name, underscore=True)
    tb.set_index(["country", "year", "field"], inplace=True)

    # Create proportion of patents granted (in time) and citations per article (total across years)
    tb["proportion_patents_granted"] = ((tb["num_patent_granted"] / tb["num_patent_applications"]) * 100).astype(float)
    tb["proportion_patents_granted"] = tb["proportion_patents_granted"].astype(float)
    tb["citations_per_article"] = (tb["num_citations_summary"] / tb["num_articles_summary"]).astype(float)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


# Define a custom aggregation function that returns NaN if all values are NaN, else returns the sum
def sum_with_nan(values):
    if values.isnull().all():
        return np.nan
    else:
        return values.sum()

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
    tb_with_regions = tb.copy()
    aggregations = {column: "sum" for column in tb_with_regions.columns if column not in ["country", "year", "field"]}

    # Find members of current region.
    members = geo.list_members_of_region(
        region="World",
        ds_regions=ds_regions,
    )
    tb_with_regions = geo.add_region_aggregates(
        df=tb_with_regions,
        region="World",
        countries_in_region=members,
        countries_that_must_have_data=[],
        num_allowed_nans_per_year=None,
        frac_allowed_nans_per_year=0.99999,
        aggregations=aggregations,
    )

    return tb_with_regions


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
    merged_total = add_world(tb=tb, ds_regions=ds_regions)

    # List of columns to include for conversion to millions (investment values)
    _investment_cols = [col for col in merged_total.columns if "investment" in col]
    # Convert all other columns to million
    merged_total.loc[:, _investment_cols] *= 1e6

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
    df_cpi_inv = pd.merge(df_wdi_cpi_us, merged_total, on="year", how="inner")

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
    tb["proportion_patents_granted"] = (tb["num_patent_granted"] / tb["num_patent_applications"]) * 100
    tb["proportion_patents_granted"] = tb["proportion_patents_granted"].astype(float)
    tb["citations_per_article"] = tb["num_citations_summary"] / tb["num_articles_summary"]

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

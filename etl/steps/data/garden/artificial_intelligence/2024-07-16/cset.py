import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def add_world(tb: Table, ds_regions: Dataset) -> Table:
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
    df_regions = df_regions.reset_index(drop=True)

    # Define aggregation rules for each column excluding "country", "year", and "field"
    numeric_cols = [col for col in df_regions.columns if col not in ["country", "year", "field"]]

    # Group the filtered data by "year" and "field" and aggregate the data based on the defined rules
    result = df_regions.groupby(["year", "field"], observed=False)[numeric_cols].agg(sum_with_nan).reset_index()

    # Assign the aggregated data to a new country named "World"
    result["country"] = "World"

    # Concatenate the aggregated 'World' data with the original table
    tb = pr.concat([tb_with_regions, result])

    # Reset the index of the concatenated table
    tb = tb.reset_index(drop=True)

    return tb


def run(dest_dir: str) -> None:
    # Load inputs.
    ds_meadow = paths.load_dataset("cset")
    tb = ds_meadow["cset"].reset_index()

    ds_us_cpi = paths.load_dataset("us_consumer_prices")
    tb_us_cpi = ds_us_cpi["us_consumer_prices"].reset_index()

    ds_population = paths.load_dataset("population")
    tb_population = ds_population["population"].reset_index()

    ds_regions = paths.load_dataset("regions")

    # Process data.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = add_world(tb=tb, ds_regions=ds_regions)

    _investment_cols = [col for col in tb.columns if "investment" in col]
    tb[_investment_cols] = tb[_investment_cols].astype("float64")
    tb.loc[:, _investment_cols] *= 1e6

    cpi_2021 = tb_us_cpi.loc[tb_us_cpi["year"] == 2021, "all_items"].values[0]
    tb_us_cpi["cpi_adj_2021"] = tb_us_cpi["all_items"] / cpi_2021
    tb_us_cpi_2021 = tb_us_cpi[["cpi_adj_2021", "year"]].copy()
    tb_cpi_inv = pr.merge(tb, tb_us_cpi_2021, on="year", how="inner")

    for col in _investment_cols:
        tb_cpi_inv[col] = round(tb_cpi_inv[col] / tb_cpi_inv["cpi_adj_2021"])

    tb_cpi_inv = tb_cpi_inv.drop("cpi_adj_2021", axis=1)

    tb = pr.merge(
        tb_cpi_inv,
        tb_population[["country", "year", "population"]].astype({"population": "float64"}),
        how="left",
        on=["country", "year"],
    )

    for col in ["num_patent_applications", "num_patent_granted", "num_articles"]:
        tb[f"{col}_per_mil"] = tb[col] / (tb["population"] / 1e6)

    tb = tb.drop("population", axis=1)

    tb["proportion_patents_granted"] = ((tb["num_patent_granted"] / tb["num_patent_applications"]) * 100).astype(float)
    tb["citations_per_article"] = (tb["num_citations_summary"] / tb["num_articles_summary"]).astype(float)

    condition = (tb["num_articles_summary"] < 1000) & pd.notna(tb["num_articles_summary"])
    tb.loc[condition, "citations_per_article"] = pd.NA

    tb = tb.format(["country", "year", "field"])

    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()


# Define a custom aggregation function that returns NaN if all values are NaN, else returns the sum
def sum_with_nan(values):
    if values.isnull().all():
        return np.nan
    else:
        return values.sum()

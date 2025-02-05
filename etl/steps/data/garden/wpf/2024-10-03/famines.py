"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions for which aggregates will be created.
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]

# Custom regions for specific places where famines occured
CUSTOM_REGION_DICT = {
    "Persia": "Asia",
    "Congo Free State": "Africa",
    "Sudan, Ethiopia": "Africa",
    "Ottoman Empire": "Asia",
    "East Africa": "Africa",
    "Somaliland": "Africa",
    "African Red Sea Region": "Africa",
    "Sahel": "Africa",
    "German East Africa": "Africa",
    "Serbia, Balkans": "Europe",
    "Greater Syria": "Asia",
    "Russia, Ukraine": "Asia",
    "Russia, Kazakhstan": "Asia",
    "Germany, USSR": "Asia",
    "East Asia": "Asia",
    "India, Bangladesh": "Asia",
    "Eastern Europe": "Europe",
    "USSR (Hungerplan)": "Asia",
    "Somaliland, African Red Sea Region": "Africa",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("famines")

    # Read regions
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["famines"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Process the cause columns to create new columns for Conflict, Government policy overall, and External factors.
    tb = process_causes(tb)

    # Add regions to the table.
    tb = add_regions(tb, ds_regions)

    # Ensure there are no NaNs in the 'region' column
    assert not tb["region"].isna().any(), "There are NaN values in the 'region' column"

    # Split and convert the 'date' column to lists of integers
    tb["date"] = tb["date"].astype(str)
    tb["date_list"] = tb["date"].apply(lambda x: [int(year) for year in x.split(",")])

    # Create a new column 'date_range' with the minimum and maximum years
    tb["date_range"] = tb["date_list"].apply(lambda x: f"{min(x)}" if min(x) == max(x) else f"{min(x)}-{max(x)}")
    tb["simplified_place"] = tb["simplified_place"].astype(str)

    # Create a new column with famine names that combines dates and simplified places
    tb["famine_name"] = tb["simplified_place"] + " " + tb["date_range"]

    # Add origins metadata to new columns.
    for col in [
        "wpf_authoritative_mortality_estimate",
        "conflict",
        "government_policy_overall",
        "external_factors",
        "famine_name",
    ]:
        tb[col].metadata.origins = tb["simplified_place"].metadata.origins

    # Drop columns that are not needed.
    tb = tb.drop(columns=["date_list", "date_range", "simplified_place"])
    tb = tb.format(["famine_name", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_causes(tb):
    """
    Process the cause columns to create new columns for Conflict, Government policy overall, and External factors.
    """
    # Define keywords for each new column
    conflict_keywords = ["war", "blockade", "counterinsurgency", "occupation", "siege", "forced starvation", "genocide"]
    government_policy_keywords = [
        "economic policy",
        "government policy",
        "colonial policy",
        "state capacity",
        "taxation",
        "forced labor",
    ]
    external_factors_keywords = ["climate", "environment", "disease"]

    # Function to check if any keyword is in the text
    def contains_keywords(text, keywords):
        if pd.isna(text):
            return 0
        return int(any(keyword in text.lower() for keyword in keywords))

    # Apply the function to each row for each new column
    tb["conflict"] = tb.apply(
        lambda row: int(
            any(
                contains_keywords(row[col], conflict_keywords)
                for col in ["cause__immediate_trigger", "cause__contributing_factors", "cause__structural_factors"]
            )
        ),
        axis=1,
    )
    tb["government_policy_overall"] = tb.apply(
        lambda row: int(
            any(
                contains_keywords(row[col], government_policy_keywords)
                for col in ["cause__immediate_trigger", "cause__contributing_factors", "cause__structural_factors"]
            )
        ),
        axis=1,
    )
    tb["external_factors"] = tb.apply(
        lambda row: int(
            any(
                contains_keywords(row[col], external_factors_keywords)
                for col in ["cause__immediate_trigger", "cause__contributing_factors", "cause__structural_factors"]
            )
        ),
        axis=1,
    )

    tb = tb.drop(columns=["cause__immediate_trigger", "cause__contributing_factors", "cause__structural_factors"])

    return tb


def add_regions(tb: Table, ds_regions: Dataset) -> Table:
    """
    Add regions to the famine data table.
    """
    # First assign custom regions
    tb["region"] = tb["simplified_place"].map(CUSTOM_REGION_DICT)

    # Add the rest as usual
    for region in REGIONS:
        # List of countries in region.
        countries_in_region = geo.list_members_of_region(region=region, ds_regions=ds_regions)

        # Add region to the table.
        tb.loc[tb["simplified_place"].isin(countries_in_region), "region"] = region

    return tb

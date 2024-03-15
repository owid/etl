"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = {
    # Default continents.
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
}

INCOME_GROUPS = {  # Income groups.
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
}


def run(dest_dir: str) -> None:
    # Load datasets.
    ds_data = paths.load_dataset("gender_statistics")
    tb = ds_data["gender_statistics"].reset_index()
    tb_data = tb[["country", "year", "se_prm_uner"]]  # Interested columns.

    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("population")
    tb_population = ds_population["population"].reset_index()

    # Clean data to include only years with data.
    years_with_data = tb_data.dropna(subset=["se_prm_uner"])["year"].unique()
    tb_data = tb_data[tb_data["year"].isin(years_with_data)]

    # Map countries to their regions and income groups.
    country_to_region = {
        country: region for region in REGIONS for country in geo.list_members_of_region(region, ds_regions)
    }
    tb_data["region"] = tb_data["country"].map(country_to_region)

    country_to_income_group = {
        country: income_group
        for income_group in INCOME_GROUPS
        for country in geo.list_members_of_region(income_group, ds_regions, ds_income_groups)
    }
    tb_data["income_group"] = tb_data["country"].map(country_to_income_group)

    # Merge with population data.
    tb_merged = pr.merge(tb_data, tb_population, on=["country", "year"]).drop(columns="source")

    # Flag for 'se_prm_uner' data presence.
    tb_merged["se_prm_uner"] = np.where(tb_merged["se_prm_uner"].isna(), 0, 1)

    # Function to calculate missing data details.
    def calculate_details(group_by_column):
        # Group by year and group, then calculate total countries.
        total_countries = tb_merged.groupby(["year", group_by_column]).size().reset_index(name="total_countries")

        # Calculate missing data for countries and population.
        missing_data = (
            tb_merged[tb_merged["se_prm_uner"] == 0]
            .groupby(["year", group_by_column])
            .agg(missing_countries=("country", "size"), missing_population=("population", "sum"))
            .reset_index()
        )

        # Merge with total countries and calculate fractions.
        detailed_data = pr.merge(total_countries, missing_data, on=["year", group_by_column])
        detailed_data["fraction_missing_countries"] = (
            detailed_data["missing_countries"] / detailed_data["total_countries"]
        ) * 100

        # Rename columns for consistency.
        return detailed_data.rename(columns={group_by_column: "region"})

    # Calculate missing details for each region and income group.
    region_details = calculate_details("region")
    income_details = calculate_details("income_group")

    # Combine and prepare the final dataset.
    df_final = pr.concat([region_details, income_details], axis=0)
    df_final = df_final.set_index(["region", "year"], verify_integrity=True)
    tb_garden = Table(df_final, short_name="children_out_of_school")

    # Ensure metadata is correctly associated.
    for column in tb_garden.columns:
        tb_garden[column].metadata.origins = tb_data["se_prm_uner"].metadata.origins

    # Save the final dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_garden], check_variables_metadata=True)
    ds_garden.save()

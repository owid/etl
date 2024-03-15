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
    tb_data = tb[["country", "year", "se_prm_uner"]]  # Just the columns we need.

    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("population")
    tb_population = ds_population["population"].reset_index()

    # Clean data to include only years with data.
    years_with_data = tb_data.dropna(subset=["se_prm_uner"])["year"].unique()
    tb_data = tb_data[tb_data["year"].isin(years_with_data)]

    # Map countries to their regions.
    country_to_region = {
        country: region for region in REGIONS for country in geo.list_members_of_region(region, ds_regions)
    }
    tb_data["region"] = tb_data["country"].map(country_to_region)

    # Map countries to their income groups.
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

    # Calculate missing data by region and income group.
    def calculate_missing_data(group_by_column):
        total = tb_merged.groupby(["year", group_by_column]).size().reset_index(name="total_countries")
        missing = (
            tb_merged[tb_merged["se_prm_uner"] == 0]
            .groupby(["year", group_by_column])
            .size()
            .reset_index(name="missing_countries")
        )
        missing["fraction_missing"] = (missing["missing_countries"] / total["total_countries"]) * 100
        return missing.rename(columns={group_by_column: "region"})

    region_missing = calculate_missing_data("region")
    income_missing = calculate_missing_data("income_group")

    # Combine and prepare the final dataset.
    df_final = pr.concat([region_missing, income_missing], axis=0).set_index(["region", "year"], verify_integrity=True)
    tb_garden = Table(df_final, short_name="children_out_of_school")

    for column in tb_garden.columns:
        tb_garden[column].metadata.origins = tb_data["se_prm_uner"].metadata.origins

    # Save the final dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_garden], check_variables_metadata=True)
    ds_garden.save()

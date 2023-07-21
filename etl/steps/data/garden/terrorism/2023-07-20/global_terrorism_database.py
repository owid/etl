"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions for which aggregates will be created.
REGIONS = ["North America", "South America", "Europe", "European Union (27)", "Africa", "Asia", "Oceania"]


def add_data_for_regions(tb: Table, regions: List[str], ds_regions: Dataset) -> Table:
    tb_with_regions = tb.copy()
    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
        )
        tb_with_regions = geo.add_region_aggregates(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.99999,
        )

    return tb_with_regions


def add_deaths_and_population(tb_terrorism):
    # Load population and merge with terrorism
    ds_population = cast(Dataset, paths.load_dependency("population"))
    tb_population = ds_population["population"].reset_index(drop=False)
    df_pop_add = pd.merge(
        tb_terrorism, tb_population[["country", "year", "population"]], how="left", on=["country", "year"]
    )

    # Load deaths and merge with terrorism
    ds_meadow_un = cast(Dataset, paths.load_dependency("un_wpp"))

    tb_un = ds_meadow_un["un_wpp"]
    deaths_df = tb_un.xs("deaths", level="metric")
    # Select rows where 'sex' is 'all'
    deaths_df_all_sex = deaths_df.xs("all", level="sex")
    # Select rows where 'variant' is 'estimates'
    deaths_df_estimates = deaths_df_all_sex.xs("estimates", level="variant")
    # Select rows where 'age' is 'all'
    deaths_final_df = deaths_df_estimates.xs("all", level="age")
    deaths_final_df.reset_index(inplace=True)
    deaths_final_df.rename(columns={"location": "country", "value": "deaths"}, inplace=True)

    df_deaths_add = pd.merge(df_pop_add, deaths_final_df, how="left", on=["country", "year"])

    return df_deaths_add


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow_terrorism = cast(Dataset, paths.load_dependency("global_terrorism_database"))
    # Read table from meadow dataset.
    tb_terrorism = ds_meadow_terrorism["global_terrorism_database"]
    tb_terrorism.reset_index(inplace=True)
    #
    # Process data.
    #
    tb: Table = geo.harmonize_countries(
        df=tb_terrorism,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )

    total_df = pd.DataFrame()
    total_df["total_killed"] = tb.groupby(["country", "year"])["nkill"].sum()
    total_df["total_wounded"] = tb.groupby(["country", "year"])["nwound"].sum()
    total_df["total_incident_counts"] = tb.groupby(["country", "year"]).size()
    total_df["total_casualties"] = total_df["total_wounded"] + total_df["total_incident_counts"]

    df_pop_deaths = add_deaths_and_population(total_df)
    # Add region aggregates.
    # Load regions dataset.
    ds_regions: Dataset = paths.load_dependency("regions")
    df_pop_deaths = add_data_for_regions(tb=df_pop_deaths, regions=REGIONS, ds_regions=ds_regions)

    df_pop_deaths["terrorism_wounded_per_capita"] = df_pop_deaths["total_wounded"] / df_pop_deaths["population"]
    df_pop_deaths["terrorism_deaths_per_capita"] = df_pop_deaths["total_killed"] / df_pop_deaths["population"]
    df_pop_deaths["terrorism_casualties_per_capita"] = df_pop_deaths["total_casualties"] / df_pop_deaths["population"]
    df_pop_deaths["terrorism_share_of_deaths"] = (df_pop_deaths["total_killed"] / df_pop_deaths["deaths"]) * 100

    tb_garden = Table(df_pop_deaths, short_name=paths.short_name)
    tb_garden.set_index(["country", "year"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow_terrorism.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

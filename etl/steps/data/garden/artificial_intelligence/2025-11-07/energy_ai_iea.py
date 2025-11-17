"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def duplicate_2024_values_for_scenarios(tb: Table) -> Table:
    """
    Duplicate 2024 historical values for each non-historical scenario.

    This ensures that when plotting scenario lines, they all start from the same
    2024 baseline point, avoiding breaks in the visualization.
    """

    # Filter for 2024 historical data
    historical_2024 = tb[(tb["year"] == 2024) & (tb["scenario"] == "historical")].copy()

    # Get all unique scenarios except historical
    scenarios = tb[tb["scenario"] != "historical"]["scenario"].unique()

    # Create duplicate rows for each scenario
    duplicated_rows = []
    for scenario in scenarios:
        scenario_rows = historical_2024.copy()
        scenario_rows["scenario"] = scenario
        duplicated_rows.append(scenario_rows)

    duplicated_rows = pd.concat(duplicated_rows, ignore_index=True)
    duplicated_rows = Table(duplicated_rows)

    # Concatenate original table with duplicated rows
    tb = pr.concat([tb, duplicated_rows], ignore_index=True)
    return tb


def create_other_country(tb: Table) -> Table:
    """
    Create derived countries by subtracting specific regions:
    - North America (IEA) excluding United States = North America (IEA) - United States
    - Asia Pacific (IEA) excluding China = Asia Pacific (IEA) - China
    - Rest of the world = World - United States - China - Europe (IEA) - Asia Pacific excl. China (IEA)
    """
    merge_cols = ["year", "metric", "data_center_category", "infrastructure_type", "scenario"]
    new_countries = []

    # Get base country data
    us_data = tb[tb["country"] == "United States"].copy()
    china_data = tb[tb["country"] == "China"].copy()
    europe_data = tb[tb["country"] == "Europe (IEA)"].copy()

    # 1. North America (IEA) excluding United States = North America (IEA) - United States
    north_america_data = tb[tb["country"] == "North America (IEA)"].copy()
    merged_na = north_america_data.merge(
        us_data[merge_cols + ["value"]], on=merge_cols, how="left", suffixes=("_na", "_us")
    )
    merged_na["value"] = merged_na["value_na"] - merged_na["value_us"].fillna(0)

    north_america_excl_us = merged_na[merge_cols + ["value"]].copy()
    north_america_excl_us["country"] = "North America (IEA) excluding United States"
    new_countries.append(north_america_excl_us)

    # 3. Asia Pacific (IEA) excluding China = Asia Pacific (IEA) - China
    asia_pacific_data = tb[tb["country"] == "Asia Pacific (IEA)"].copy()
    merged_ap = asia_pacific_data.merge(
        china_data[merge_cols + ["value"]], on=merge_cols, how="left", suffixes=("_ap", "_china")
    )
    merged_ap["value"] = merged_ap["value_ap"] - merged_ap["value_china"].fillna(0)

    asia_pacific_excl_china = merged_ap[merge_cols + ["value"]].copy()
    asia_pacific_excl_china["country"] = "Asia Pacific excl. China (IEA)"
    new_countries.append(asia_pacific_excl_china)

    # 3. Rest of the world = World - United States - China - Europe (IEA) - Asia Pacific excl. China (IEA)
    # First add the derived Asia Pacific excl China to the table temporarily for the calculation
    tb_with_ap_excl = pr.concat([tb, Table(asia_pacific_excl_china)], ignore_index=True)
    ap_excl_china_data = tb_with_ap_excl[tb_with_ap_excl["country"] == "Asia Pacific excl. China (IEA)"].copy()

    world_data = tb[tb["country"] == "World"].copy()

    # Rename value columns before merging to avoid suffix issues
    us_data_renamed = us_data.rename(columns={"value": "value_us"})
    china_data_renamed = china_data.rename(columns={"value": "value_china"})
    europe_data_renamed = europe_data.rename(columns={"value": "value_europe"})
    ap_excl_china_data_renamed = ap_excl_china_data.rename(columns={"value": "value_ap_excl"})

    merged = world_data.merge(us_data_renamed[merge_cols + ["value_us"]], on=merge_cols, how="left")
    merged = merged.merge(china_data_renamed[merge_cols + ["value_china"]], on=merge_cols, how="left")
    merged = merged.merge(europe_data_renamed[merge_cols + ["value_europe"]], on=merge_cols, how="left")
    merged = merged.merge(ap_excl_china_data_renamed[merge_cols + ["value_ap_excl"]], on=merge_cols, how="left")

    merged["value"] = (
        merged["value"]
        - merged["value_us"].fillna(0)
        - merged["value_china"].fillna(0)
        - merged["value_europe"].fillna(0)
        - merged["value_ap_excl"].fillna(0)
    )

    rest_of_world = merged[merge_cols + ["value"]].copy()
    rest_of_world["country"] = "Rest of the world"
    new_countries.append(rest_of_world)

    # Concatenate all new countries with original table
    tb = pr.concat([tb] + [Table(nc) for nc in new_countries], ignore_index=True)

    return tb


def calculate_share_of_total_demand(tb: Table, tb_electricity: Table) -> Table:
    """
    Calculate share of total electricity demand for regional total electricity consumption only.

    Only calculates for: metric="Regional total electricity consumption (TWh)",
    data_center_category="Total", infrastructure_type="Total", scenario="historical".

    Creates a new "Rest of the World excl. China and United States" aggregate for electricity demand,
    then calculates the percentage share for World and Rest of the World.
    """
    # Filter for specific metric combination only
    tb_filtered = tb[
        (tb["metric"] == "Regional total electricity consumption (TWh)")
        & (tb["data_center_category"] == "Total")
        & (tb["infrastructure_type"] == "Total")
        & (tb["scenario"] == "historical")
    ].copy()

    # Get relevant columns from electricity mix dataset
    tb_elec = tb_electricity.reset_index()[["country", "year", "total_demand__twh"]].copy()

    # Create "Rest of the World excl. China and United States" for electricity demand
    world_elec = tb_elec[tb_elec["country"] == "World"].copy()
    china_elec = tb_elec[tb_elec["country"] == "China"].copy()
    us_elec = tb_elec[tb_elec["country"] == "United States"].copy()

    # Merge and subtract
    rest_world_elec = world_elec.merge(
        china_elec[["year", "total_demand__twh"]], on="year", how="left", suffixes=("", "_china")
    )
    rest_world_elec = rest_world_elec.merge(
        us_elec[["year", "total_demand__twh"]], on="year", how="left", suffixes=("", "_us")
    )
    rest_world_elec["total_demand__twh"] = (
        rest_world_elec["total_demand__twh"]
        - rest_world_elec["total_demand__twh_china"].fillna(0)
        - rest_world_elec["total_demand__twh_us"].fillna(0)
    )
    rest_world_elec["country"] = "Rest of the world excl. China and United States"
    rest_world_elec = rest_world_elec[["country", "year", "total_demand__twh"]]

    # Combine all electricity demand data
    tb_elec_all = pr.concat([tb_elec, rest_world_elec], ignore_index=True)

    # Filter for World and Rest of the world only
    countries_to_match = ["World", "Rest of the world"]
    tb_filtered = tb_filtered[tb_filtered["country"].isin(countries_to_match)].copy()

    # Create mapping for Rest of the world
    tb_filtered_reset = tb_filtered.reset_index()
    tb_filtered_reset.loc[tb_filtered_reset["country"] == "Rest of the world", "country"] = (
        "Rest of the world excl. China and United States"
    )

    # Merge AI energy with total electricity demand
    merge_cols = ["country", "year"]
    tb_merged = tb_filtered_reset.merge(
        tb_elec_all[["country", "year", "total_demand__twh"]], on=merge_cols, how="left"
    )

    # Calculate share as percentage
    tb_merged["value_share"] = (tb_merged["value"] / tb_merged["total_demand__twh"]) * 100

    # Create new metric name
    tb_merged["metric"] = "Regional total electricity consumption (share of total electricity demand)"

    # Restore country name for Rest of the world
    tb_merged.loc[tb_merged["country"] == "Rest of the world excl. China and United States", "country"] = (
        "Rest of the world"
    )

    # Keep only the share data with original columns
    tb_share = tb_merged[
        ["country", "year", "metric", "data_center_category", "infrastructure_type", "scenario", "value_share"]
    ].copy()
    tb_share = tb_share.rename(columns={"value_share": "value"})

    return Table(tb_share)


def run() -> None:
    """Load IEA Energy and AI meadow dataset and create a garden dataset."""
    paths.log.info("energy_ai_iea.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("energy_ai_iea")

    # Read the combined table from meadow
    tb = ds_meadow.read("energy_ai_iea")

    # Load electricity mix dataset for total demand data
    ds_electricity = paths.load_dataset("electricity_mix")
    tb_electricity = ds_electricity.read("electricity_mix")

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Duplicate 2024 historical values for each scenario to avoid line breaks in visualizations
    tb = duplicate_2024_values_for_scenarios(tb)

    # Create "Other" country (World - United States - China)
    tb = create_other_country(tb)

    # Calculate share of total electricity demand
    tb_share = calculate_share_of_total_demand(tb, tb_electricity)
    print(tb_share.columns)
    # Combine original data with share data
    tb_combined = pr.concat([tb, tb_share], ignore_index=True)

    # Format with proper index
    tb_combined = tb_combined.format(
        ["country", "year", "metric", "data_center_category", "infrastructure_type", "scenario"]
    )
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("energy_ai_iea.end")

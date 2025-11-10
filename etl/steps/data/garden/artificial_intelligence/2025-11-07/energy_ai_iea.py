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

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Duplicate 2024 historical values for each scenario to avoid line breaks in visualizations
    tb = duplicate_2024_values_for_scenarios(tb)

    # Create "Other" country (World - United States - China)
    tb = create_other_country(tb)

    # Format with proper index
    tb = tb.format(["country", "year", "metric", "data_center_category", "infrastructure_type", "scenario"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("energy_ai_iea.end")

"""Load a meadow dataset and create a garden dataset."""

from functools import reduce

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Constants
HISTORICAL_SCENARIO = "historical"
TOTAL_ELEC_CONSUMPTION_METRIC = "Total electricity consumption (TWh)"
SHARE_METRIC = "Total electricity consumption (share of total electricity demand)"

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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

    #
    # Process data.
    #
    # Harmonize country names
    tb = paths.regions.harmonize_names(tb)

    # Duplicate 2024 historical values for each scenario to avoid line breaks in visualizations
    tb = duplicate_2024_values_for_scenarios(tb)

    # Create "Other" country (World - United States - China)
    tb = create_other_country(tb)

    # Calculate share of total electricity demand
    tb_share = calculate_share_of_total_demand(tb, tb_electricity)

    # Combine original data with share data
    tb_combined = pr.concat([tb, tb_share], ignore_index=True)

    # Create column names from metric and scenario combinations
    tb_combined["column_name"] = tb_combined["metric"] + " - " + tb_combined["scenario"]

    # Pivot to wide format: each metric-scenario combination becomes a column
    tb_wide = tb_combined.pivot(index=["country", "year"], columns="column_name", values="value").reset_index()

    ####################################################################################################################
    # # TODO: Alternative method. Uncomment if agreed.
    # # Add custom regions (e.g. "World excl. US and China").
    # tb = add_custom_regions(tb=tb)

    # # Add rows for electricity consumption (from IEA) as a share of electricity demand (from Ember).
    # tb_share = create_share_of_electricity_demand(tb=tb, tb_electricity=tb_electricity)

    # # Create column names from metric and scenario combinations
    # tb["column_name"] = tb["metric"] + " - " + tb["scenario"]

    # # Pivot to wide format: each metric-scenario combination becomes a column
    # tb_wide = tb.pivot(index=["country", "year"], columns="column_name", values="value", join_column_levels_with="")

    # # Combine table of total values with table of share values.
    # tb_wide = tb_wide.merge(
    #     tb_share[["country", "year", "value"]].rename(columns={"value": f"{SHARE_METRIC} - {HISTORICAL_SCENARIO}"}),
    #     on=["country", "year"],
    #     how="outer",
    # )

    ####################################################################################################################

    # Format
    tb_wide = tb_wide.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_wide], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("energy_ai_iea.end")


def duplicate_2024_values_for_scenarios(tb: Table) -> Table:
    """
    Duplicate 2024 historical values for each non-historical scenario.

    This ensures that when plotting scenario lines, they all start from the same
    2024 baseline point, avoiding breaks in the visualization.
    """

    # Filter for 2024 historical data
    historical_2024 = tb[(tb["year"] == 2024) & (tb["scenario"] == HISTORICAL_SCENARIO)].copy()

    # Get all unique scenarios except historical
    scenarios = tb[tb["scenario"] != HISTORICAL_SCENARIO]["scenario"].unique()

    # Create duplicate rows for each scenario
    duplicated_rows = []
    for scenario in scenarios:
        scenario_rows = historical_2024.copy()
        scenario_rows["scenario"] = scenario
        duplicated_rows.append(scenario_rows)

    # Concatenate all duplicated rows
    duplicated_rows = pr.concat(duplicated_rows, ignore_index=True)

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
    merge_cols = ["year", "metric", "scenario"]
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

    # 2. Asia Pacific (IEA) excluding China = Asia Pacific (IEA) - China
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

    # Prepare data for merging with renamed value columns
    dfs_to_merge = [
        world_data,
        us_data[merge_cols + ["value"]].rename(columns={"value": "value_us"}),
        china_data[merge_cols + ["value"]].rename(columns={"value": "value_china"}),
        europe_data[merge_cols + ["value"]].rename(columns={"value": "value_europe"}),
        ap_excl_china_data[merge_cols + ["value"]].rename(columns={"value": "value_ap_excl"}),
    ]

    # Use reduce to merge all dataframes at once
    merged = reduce(lambda left, right: left.merge(right, on=merge_cols, how="left"), dfs_to_merge)

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

    # Concatenate all new countries with original table (convert to Table)
    new_countries_tables = [Table(nc) for nc in new_countries]
    tb = pr.concat([tb] + new_countries_tables, ignore_index=True)

    return tb


def calculate_share_of_total_demand(tb: Table, tb_electricity: Table) -> Table:
    """
    Calculate share of total electricity demand for total electricity consumption only.

    Only calculates for: metric="Total electricity consumption (TWh)", scenario="historical".

    Creates a new "Rest of the World excl. China and United States" aggregate for electricity demand,
    then calculates the percentage share for World and Rest of the World.
    """
    # Filter for specific metric combination only
    tb_filtered = tb[(tb["metric"] == TOTAL_ELEC_CONSUMPTION_METRIC) & (tb["scenario"] == HISTORICAL_SCENARIO)]

    # Get relevant columns from electricity mix dataset
    tb_elec = tb_electricity.reset_index()[["country", "year", "total_demand__twh"]]

    # Create "Rest of the World excl. China and United States" for electricity demand
    world_elec = tb_elec[tb_elec["country"] == "World"]
    china_elec = tb_elec[tb_elec["country"] == "China"]
    us_elec = tb_elec[tb_elec["country"] == "United States"]

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

    # Filter for countries with complete electricity demand data and reset index
    countries_to_match = ["World", "Rest of the world", "China", "United States"]
    tb_filtered = tb_filtered[tb_filtered["country"].isin(countries_to_match)].reset_index()

    # Create country name mapping for merging
    country_mapping = {"Rest of the world": "Rest of the world excl. China and United States"}
    tb_filtered["country_mapped"] = tb_filtered["country"].replace(country_mapping)

    # Merge AI energy with total electricity demand using mapped country names
    tb_merged = tb_filtered.merge(
        tb_elec_all[["country", "year", "total_demand__twh"]],
        left_on=["country_mapped", "year"],
        right_on=["country", "year"],
        how="left",
        suffixes=("", "_elec"),
    )

    # Calculate share as percentage
    tb_merged["value"] = (tb_merged["value"] / tb_merged["total_demand__twh"]) * 100

    # Create new metric name
    tb_merged["metric"] = SHARE_METRIC

    # Keep only the share data with original columns (using original country names)
    tb_share = tb_merged[["country", "year", "metric", "scenario", "value"]]

    return Table(tb_share)


def add_custom_regions(tb):
    """
    Add custom regions:
    - North America (IEA) excl. United States = North America (IEA) - United States
    - Asia Pacific (IEA) excl. China = Asia Pacific (IEA) - China
    - World excl. United States and China = World - United States - China
    """
    tb = tb.copy()

    # Create a temporary table for US with negative values (to be subtracted later when creating aggregates).
    tb_us_subtracted = (
        tb[tb["country"] == "United States"].reset_index(drop=True).assign(**{"country": "United States subtracted"})
    )
    tb_us_subtracted["value"] *= -1

    # Idem for China.
    tb_china_subtracted = tb[tb["country"] == "China"].reset_index(drop=True).assign(**{"country": "China subtracted"})
    tb_china_subtracted["value"] *= -1

    # Combine original table with temporary ones.
    tb = pr.concat([tb, tb_us_subtracted, tb_china_subtracted], ignore_index=True)

    # Create a custom aggregate region for World excluding United States and China.
    tb = paths.regions.add_aggregates(
        tb=tb,
        index_columns=["country", "year", "metric", "scenario"],
        regions={
            "World excl. United States and China": {
                "custom_members": ["World", "United States subtracted", "China subtracted"]
            }
        },
    )

    # Create a custom aggregate region for Asia excluding China.
    tb = paths.regions.add_aggregates(
        tb=tb,
        index_columns=["country", "year", "metric", "scenario"],
        regions={"Asia Pacific (IEA) excl. China": {"custom_members": ["Asia Pacific (IEA)", "China subtracted"]}},
    )

    # Create a custom aggregate region for North America excluding United States.
    tb = paths.regions.add_aggregates(
        tb=tb,
        index_columns=["country", "year", "metric", "scenario"],
        regions={
            "North America (IEA) excl. United States": {
                "custom_members": ["North America (IEA)", "United States subtracted"]
            }
        },
    )

    # Remove temporary rows.
    tb = tb[~tb["country"].str.contains("subtracted")].reset_index(drop=True)

    return tb


def create_share_of_electricity_demand(tb, tb_electricity):
    # Create a table of electricity demand (defined by Ember as electricity generation minus net imports) for selected countries.
    tb_demand = (
        tb_electricity[tb_electricity["country"].isin(["World", "United States", "China"])][
            ["country", "year", "total_demand__twh"]
        ]
        .dropna()
        .reset_index(drop=True)
    )

    # Create a temporary table for the demand of China and US.
    tb_demand_us_china = (
        tb_demand[tb_demand["country"].isin(["United States", "China"])]
        .groupby(["year"], as_index=False)
        .agg({"total_demand__twh": "sum"})
    )

    # Create a temporary table for the demand of all countries except China and US.
    tb_demand_rest = (
        tb_demand[tb_demand["country"] == "World"]
        .drop(columns=["country"])
        .merge(tb_demand_us_china, on="year", how="inner", suffixes=("_world", "_us_china"))
    )
    tb_demand_rest["total_demand__twh"] = (
        tb_demand_rest["total_demand__twh_world"] - tb_demand_rest["total_demand__twh_us_china"]
    )
    tb_demand_rest["country"] = "World excl. United States and China"

    # Combine the original demand table with the one that includes World excl. US and China.
    tb_demand = pr.concat([tb_demand, tb_demand_rest[["country", "year", "total_demand__twh"]]], ignore_index=True)

    # Create a new table with the electricity consumption of China, US, World, and rest of the world, as a percentage of their total electricity demand.
    tb_share = tb[
        (tb["metric"] == TOTAL_ELEC_CONSUMPTION_METRIC)
        & (tb["scenario"] == HISTORICAL_SCENARIO)
        & (tb["country"].isin(["World", "China", "United States", "World excl. United States and China"]))
    ].reset_index(drop=True)
    tb_share = tb_share.merge(tb_demand, on=["country", "year"], how="left")
    tb_share["value"] = 100 * tb_share["value"] / tb_share["total_demand__twh"]
    tb_share["metric"] = SHARE_METRIC
    tb_share = tb_share.drop(columns=["total_demand__twh"])

    return tb_share

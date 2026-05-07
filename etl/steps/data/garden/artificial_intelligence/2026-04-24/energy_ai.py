"""Load a meadow dataset and create a garden dataset."""

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
    paths.log.info("energy_ai.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("energy_ai")

    # Read the combined table from meadow
    tb = ds_meadow.read("energy_ai")

    # Load electricity mix dataset for total demand data
    ds_electricity = paths.load_dataset("electricity_mix")
    tb_electricity = ds_electricity.read("electricity_mix")

    #
    # Process data.
    #
    # Harmonize country names
    tb = paths.regions.harmonize_names(tb)

    # Duplicate latest historical year values for each scenario to avoid line breaks in visualizations
    tb = duplicate_latest_historical_year_for_scenarios(tb)

    # Add custom regions (e.g. "World excl. US and China").
    tb = add_custom_regions(tb=tb)

    # Add rows for electricity consumption (from IEA) as a share of electricity demand (from Ember).
    tb_share = create_share_of_electricity_demand(tb=tb, tb_electricity=tb_electricity)

    # Create column names from metric and scenario combinations
    tb["column_name"] = tb["metric"] + " - " + tb["scenario"]

    # Pivot to wide format: each metric-scenario combination becomes a column
    tb_wide = tb.pivot(index=["country", "year"], columns="column_name", values="value", join_column_levels_with="")

    # Combine table of total values with table of share values.
    tb_wide = tb_wide.merge(
        tb_share[["country", "year", "value"]].rename(columns={"value": f"{SHARE_METRIC} - {HISTORICAL_SCENARIO}"}),
        on=["country", "year"],
        how="outer",
    )

    # Format
    tb_wide = tb_wide.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_wide], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("energy_ai.end")


def duplicate_latest_historical_year_for_scenarios(tb: Table) -> Table:
    """
    Duplicate latest historical year values for each non-historical scenario.

    This ensures that when plotting scenario lines, they all start from the same
    historical baseline point, avoiding breaks in the visualization.
    """

    # Get latest historical year
    latest_historical_year = tb[tb["scenario"] == HISTORICAL_SCENARIO]["year"].max()

    # Filter for latest historical data
    historical_latest = tb[(tb["year"] == latest_historical_year) & (tb["scenario"] == HISTORICAL_SCENARIO)].copy()

    # Get all unique scenarios except historical
    scenarios = tb[tb["scenario"] != HISTORICAL_SCENARIO]["scenario"].unique()

    # Create duplicate rows for each scenario
    duplicated_rows = []
    for scenario in scenarios:
        scenario_rows = historical_latest.copy()
        scenario_rows["scenario"] = scenario
        duplicated_rows.append(scenario_rows)

    # Concatenate all duplicated rows
    duplicated_rows = pr.concat(duplicated_rows, ignore_index=True)

    # Concatenate original table with duplicated rows
    tb = pr.concat([tb, duplicated_rows], ignore_index=True)
    return tb


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

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
    Create an "Other" country by subtracting United States and China from World.

    For each combination of year, metric, data_center_category, infrastructure_type, and scenario:
    Other = World - United States - China
    """

    # Get World data
    world_data = tb[tb["country"] == "World"].copy()

    # Get US and China data
    us_data = tb[tb["country"] == "United States"].copy()
    china_data = tb[tb["country"] == "China"].copy()

    # Merge on all dimensions except country and value
    merge_cols = ["year", "metric", "data_center_category", "infrastructure_type", "scenario"]

    # Merge World with US and China
    merged = world_data.merge(us_data[merge_cols + ["value"]], on=merge_cols, how="left", suffixes=("_world", "_us"))
    merged = merged.merge(china_data[merge_cols + ["value"]], on=merge_cols, how="left")

    # After the second merge, China's value column is just "value" (no suffix)
    # Calculate Other = World - US - China
    merged["value_other"] = merged["value_world"] - merged["value_us"].fillna(0) - merged["value"].fillna(0)
    merged["value"] = merged["value_other"]

    # Keep only necessary columns
    other_data = merged[["year", "metric", "data_center_category", "infrastructure_type", "scenario", "value"]].copy()
    other_data["country"] = "Other"

    # Concatenate with original table
    tb = pr.concat([tb, Table(other_data)], ignore_index=True)

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

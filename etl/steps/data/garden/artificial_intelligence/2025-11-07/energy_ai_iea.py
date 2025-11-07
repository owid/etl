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
    # Reset index to work with the data
    tb = tb.reset_index()

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
    print(duplicated_rows)

    # Concatenate original table with duplicated rows
    tb = pr.concat([tb, duplicated_rows], ignore_index=True)
    tb = tb.drop(columns=["index"])
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

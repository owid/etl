"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define latest year without projections
LATEST_YEAR_WITHOUT_PROJECTIONS = 2024

# Define tables to be loaded. I am not processing country, because they were created for the aggregations and not to highlight them.
TABLES = ["region", "global"]

# Define scenarios and new names
SCENARIOS = {
    "historical": "Historical estimates",
    "current_forecast": "Current forecast + historical growth projections",
    "2pct": "2% growth projections",
    "2pct_gini1": "2% growth + Gini reduction 1% projections",
    "2pct_gini2": "2% growth + Gini reduction 2% projections",
    "4pct": "4% growth projections",
    "6pct": "6% growth projections",
    "8pct": "8% growth projections",
}

# Define index columns
INDEX_COLUMNS = ["country", "year", "povertyline", "scenario"]

# Define indicator columns
INDICATOR_COLUMNS = ["fgt0", "poorpop"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("poverty_projections")

    # Read tables from meadow dataset.
    tables = [ds_meadow.read(table_name) for table_name in TABLES]

    #
    # Process data.
    #
    # Concatenate tables
    tb = pr.concat(tables, ignore_index=True)

    # Round povertyline to 2 decimal places
    tb["povertyline"] = tb["povertyline"].round(2)

    # Multiply poorpop by 1_000_000
    tb["poorpop"] = tb["poorpop"] * 1_000_000

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    tb = connect_estimates_with_projections(tb)

    # Rename scenario column
    tb["scenario"] = map_series(
        series=tb["scenario"],
        mapping=SCENARIOS,
    )

    # Recover origins
    tb["scenario"] = tb["scenario"].copy_metadata(tb["country"])

    tb = tb.format(INDEX_COLUMNS, short_name="poverty_projections")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def connect_estimates_with_projections(tb: Table) -> Table:
    """
    Connects estimates with projections for visualizations in Grapher.
    This is repeating the latest estimate in the historical scenario in the rest of the scenarios.
    """

    tb = tb.copy()

    # Save tb_historical and tb_current_forecast, by filtering scenario in historical and current_forecast
    tb_historical = tb[tb["scenario"] == "historical"].copy().reset_index(drop=True)
    tb_current_forecast = tb[tb["scenario"] == "current_forecast"].copy().reset_index(drop=True)

    # Make table wider, by using scenario as columns
    tb = tb.pivot(index=["country", "year", "povertyline"], columns="scenario", values=INDICATOR_COLUMNS)

    # For year LATEST_YEAR_WITHOUT_PROJECTIONS, fill the rest of the columns with the same value
    for indicator in INDICATOR_COLUMNS:
        for scenario in SCENARIOS.keys():
            if scenario != "historical":
                tb.loc[tb.index.get_level_values("year") == LATEST_YEAR_WITHOUT_PROJECTIONS, (indicator, scenario)] = (
                    tb.loc[
                        tb.index.get_level_values("year") == LATEST_YEAR_WITHOUT_PROJECTIONS, (indicator, scenario)
                    ].combine_first(
                        tb.loc[
                            tb.index.get_level_values("year") == LATEST_YEAR_WITHOUT_PROJECTIONS,
                            (indicator, "historical"),
                        ]
                    )
                )

    # Make table long again, by creating a scenario column
    tb = tb.stack(level="scenario", future_stack=True).reset_index()

    # Recover origins
    for indicator in INDICATOR_COLUMNS:
        tb[indicator] = tb[indicator].copy_metadata(tb["country"])

    # Combine historical and current_forecast, by concatenating tb_historical and tb_current_forecast
    tb_connected = pr.concat([tb_historical, tb_current_forecast], ignore_index=True)

    # Rename scenario column to "Historical + current forecast + historical growth"
    tb_connected["scenario"] = "Historical estimates + projections"

    # Keep only the columns in INDEX_COLUMNS and INDICATOR_COLUMNS
    tb_connected = tb_connected[INDEX_COLUMNS + INDICATOR_COLUMNS]

    # Concatenate tb and tb_connected
    tb = pr.concat([tb, tb_connected], ignore_index=True)

    return tb

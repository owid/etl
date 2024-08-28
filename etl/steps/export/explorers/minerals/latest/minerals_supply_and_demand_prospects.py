"""Load a grapher dataset and create an explorer dataset with its tsv file."""

import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_explorer

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefix used for "share" columns.
# NOTE: This must coincide with the same variable as defined in the garden minerals step.
SHARE_OF_GLOBAL_PREFIX = "share of global "


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load minerals grapher dataset and read its main table.
    ds = paths.load_dataset("critical_minerals")
    tb_demand = ds.read_table("demand_by_technology")
    tb_supply = ds.read_table("supply_by_country")

    #
    # Process data.
    #
    # Remove "World" from supply data (since all countries should always add up to World).
    # TODO: Add sanity check for this.
    tb_supply = tb_supply[tb_supply["country"] != "World"].reset_index(drop=True)

    # Combine the two tables into one (where "country" column actually means technology for demand table, and country for supply table).
    tb = tb_demand.merge(tb_supply, on=["country", "year"], how="outer")

    # Prepare graphers table of explorer.
    variable_ids = []
    metric_dropdown = []
    mineral_dropdown = []
    type_dropdown = []
    case_dropdown = []
    scenario_dropdown = []
    map_tab = []
    for column in tb.drop(columns=["country", "year"]).columns:
        metric, mineral, process, case, scenario = tb[column].metadata.title.split("|")
        metric = metric.replace("_", " ").capitalize()

        # Append extracted values.
        if column in tb_demand.columns:
            table_name = tb_demand.metadata.short_name
        else:
            table_name = tb_supply.metadata.short_name
        variable_ids.append([f"{ds.metadata.uri}/{table_name}#{column}"])
        metric_dropdown.append(metric)
        type_dropdown.append(process)
        mineral_dropdown.append(mineral)
        case_dropdown.append(case)
        scenario_dropdown.append(scenario)
        map_tab.append(False)

    df_graphers = pd.DataFrame()
    df_graphers["yVariableIds"] = variable_ids
    df_graphers["Mineral Dropdown"] = mineral_dropdown
    df_graphers["Type Dropdown"] = type_dropdown
    df_graphers["Metric Dropdown"] = metric_dropdown
    df_graphers["Case Dropdown"] = case_dropdown
    df_graphers["Scenario Dropdown"] = scenario_dropdown
    df_graphers["hasMapTab"] = map_tab

    # Impose that all line charts start at zero.
    df_graphers["yAxisMin"] = 0

    # Make all views stacked area charts.
    df_graphers["type"] = "StackedArea"

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(
            subset=["Case Dropdown", "Scenario Dropdown", "Mineral Dropdown", "Type Dropdown", "Metric Dropdown"],
            keep=False,
        )
    ].empty, error

    # Sort rows conveniently.
    # df_graphers["Metric Dropdown"] = pd.Categorical(
    #     df_graphers["Metric Dropdown"],
    #     categories=["Production", "Reserves", "Unit value"],
    #     ordered=True,
    # )
    # df_graphers = df_graphers.sort_values(["Mineral Dropdown", "Metric Dropdown", "Type Dropdown"]).reset_index(
    #     drop=True
    # )

    # Choose which indicator to show by default when opening the explorer.
    # df_graphers["defaultView"] = False
    # df_graphers.loc[
    #     (df_graphers["Mineral Dropdown"] == "Copper")
    #     & (df_graphers["Type Dropdown"] == "Mine")
    #     & (df_graphers["Metric Dropdown"] == "Production")
    #     & (~df_graphers["Share of global Checkbox"]),
    #     "defaultView",
    # ] = True

    # Prepare explorer metadata.
    config = {
        "explorerTitle": "Minerals Supply and Demand Prospects",
        "explorerSubtitle": "",
        # Ensure all entities (countries or technologies) are selected by default, so that stacked area charts are always showing totals.
        "selection": sorted(set(tb["country"])),
    }

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.save()

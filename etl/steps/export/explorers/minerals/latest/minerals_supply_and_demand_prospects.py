"""Load a grapher dataset and create an explorer dataset with its tsv file."""

import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_explorer

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    # Combine the two tables into one (where "country" column actually means technology for demand table, and country for supply table).
    tb = tb_demand.merge(tb_supply, on=["country", "year"], how="outer")

    # Prepare graphers table of explorer.
    variable_ids = []
    indicator_radio = []
    metric_radio = []
    mineral_dropdown = []
    type_dropdown = []
    case_dropdown = []
    scenario_dropdown = []
    map_tab = []
    for column in tb.drop(columns=["country", "year"]).columns:
        metric, mineral, process, case, scenario = tb[column].metadata.title.split("|")

        # Append extracted values.
        if column in tb_demand.columns:
            indicator = "Demand by technology"
            table_name = tb_demand.metadata.short_name
            has_map_tab = False
        else:
            indicator = "Supply by country"
            table_name = tb_supply.metadata.short_name
            has_map_tab = True

        if metric in ["demand", "supply"]:
            metric = "Total"
        elif metric.endswith("_share_of_global_demand"):
            metric = "Share of demand"
        elif metric.endswith("_share_of_global_supply"):
            metric = "Share of supply"
        else:
            log.warning(f"Unexpected metric {metric}")
        variable_ids.append([f"{ds.metadata.uri}/{table_name}#{column}"])

        indicator_radio.append(indicator)
        metric_radio.append(metric)
        type_dropdown.append(process)
        mineral_dropdown.append(mineral)
        case_dropdown.append(case)
        scenario_dropdown.append(scenario)
        map_tab.append(has_map_tab)

    df_graphers = pd.DataFrame()
    df_graphers["yVariableIds"] = variable_ids
    df_graphers["Indicator Radio"] = indicator_radio
    df_graphers["Mineral Dropdown"] = mineral_dropdown
    df_graphers["Type Dropdown"] = type_dropdown
    df_graphers["Case Dropdown"] = case_dropdown
    df_graphers["Scenario Dropdown"] = scenario_dropdown
    df_graphers["Metric Radio"] = metric_radio
    df_graphers["hasMapTab"] = map_tab

    # Impose that all line charts start at zero.
    df_graphers["yAxisMin"] = 0

    # Make all views stacked area charts.
    df_graphers["type"] = "StackedArea"

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(
            subset=[
                "Indicator Radio",
                "Case Dropdown",
                "Scenario Dropdown",
                "Mineral Dropdown",
                "Type Dropdown",
                "Metric Radio",
            ],
            keep=False,
        )
    ].empty, error

    # Sort rows conveniently.
    df_graphers = df_graphers.sort_values(
        ["Indicator Radio", "Mineral Dropdown", "Type Dropdown", "Case Dropdown", "Scenario Dropdown", "Metric Radio"]
    ).reset_index(drop=True)

    # Choose which indicator to show by default when opening the explorer.
    df_graphers["defaultView"] = False
    default_mask = (
        (df_graphers["Indicator Radio"] == "Demand by technology")
        & (df_graphers["Mineral Dropdown"] == "Copper")
        & (df_graphers["Type Dropdown"] == "Refinery")
        & (df_graphers["Case Dropdown"] == "Base case")
        & (df_graphers["Scenario Dropdown"] == "Net zero by 2050")
        & (df_graphers["Metric Radio"] == "Total")
    )
    assert len(df_graphers[default_mask]) == 1, "Multiple rows selected for default view."
    df_graphers.loc[default_mask, "defaultView"] = True

    # Prepare explorer metadata.
    config = {
        "explorerTitle": "Minerals Supply and Demand Prospects",
        "explorerSubtitle": "",
        "entityType": "country or technology",
        # Ensure all entities (countries or technologies) are selected by default, so that stacked area charts are always showing totals.
        "selection": sorted(set(tb["country"])),
    }

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.save()

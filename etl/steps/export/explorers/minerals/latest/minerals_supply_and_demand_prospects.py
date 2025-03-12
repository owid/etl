"""Load a grapher dataset and create an explorer dataset with its tsv file."""

import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load minerals grapher dataset on demand by technology.
    ds_demand = paths.load_dataset("critical_minerals_demand_by_technology")
    tb_demand = ds_demand.read("demand_by_technology")

    # Load minerals grapher dataset on supply by country.
    ds_supply = paths.load_dataset("critical_minerals_supply_by_country")
    tb_supply = ds_supply.read("supply_by_country")

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
            variable_id = f"{ds_demand.metadata.uri}/{table_name}#{column}"
        else:
            indicator = "Supply by country"
            table_name = tb_supply.metadata.short_name
            has_map_tab = True
            variable_id = f"{ds_supply.metadata.uri}/{table_name}#{column}"

        if metric in ["demand", "supply"]:
            metric = "Total"
        elif metric.endswith("_share_of_global_demand"):
            metric = "Share of demand"
        elif metric.endswith("_share_of_global_supply"):
            metric = "Share of supply"
        else:
            log.warning(f"Unexpected metric {metric}")

        variable_ids.append([variable_id])
        indicator_radio.append(indicator)
        metric_radio.append(metric)
        type_dropdown.append(process)
        mineral_dropdown.append(mineral)
        case_dropdown.append(case)
        scenario_dropdown.append(scenario)
        map_tab.append(has_map_tab)

    df_graphers = pd.DataFrame()
    df_graphers["yVariableIds"] = variable_ids
    df_graphers["Mineral Dropdown"] = mineral_dropdown
    df_graphers["Type Dropdown"] = type_dropdown
    df_graphers["Indicator Radio"] = indicator_radio
    df_graphers["Metric Radio"] = metric_radio
    df_graphers["Demand Scenario Dropdown"] = scenario_dropdown
    df_graphers["Case Dropdown"] = case_dropdown
    df_graphers["hasMapTab"] = map_tab

    # Impose that all line charts start at zero.
    df_graphers["yAxisMin"] = 0

    # Make all views stacked area charts.
    df_graphers["chartTypes"] = ["StackedArea"]

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(
            subset=[
                "Indicator Radio",
                "Case Dropdown",
                "Demand Scenario Dropdown",
                "Mineral Dropdown",
                "Type Dropdown",
                "Metric Radio",
            ],
            keep=False,
        )
    ].empty, error

    # Sort rows conveniently.
    # To ensure categories appear in the right order, they corresponding column can be defined as categorical.
    # NOTE: But ensure they are defined in the same way as they appear in the data, otherwise they will not appear in the explorer.
    scenarios_sorted = ["All scenarios", "Stated policies", "Announced pledges", "Net zero by 2050"]
    error = "The defined categories do not coincide with scenarios. They will not appear in the explorer."
    assert set(df_graphers["Demand Scenario Dropdown"]) == set(scenarios_sorted), error
    df_graphers["Demand Scenario Dropdown"] = pd.Categorical(
        df_graphers["Demand Scenario Dropdown"],
        categories=scenarios_sorted,
        ordered=True,
    )
    # NOTE: Ideally, the "Type Dropdown" would be sorted as first "Mine" and then "Refinery" (for consistency with the
    # minerals explorer). However, the first mineral in this explorer is "Arsenic", which only has "Refinery" type, and
    # therefore, "Refinery" will always be the first choice in that dropdown.
    df_graphers = df_graphers.sort_values(
        [
            "Indicator Radio",
            "Mineral Dropdown",
            "Type Dropdown",
            "Demand Scenario Dropdown",
            "Case Dropdown",
            "Metric Radio",
        ]
    ).reset_index(drop=True)

    # Choose which indicator to show by default when opening the explorer.
    df_graphers["defaultView"] = False
    default_mask = (
        (df_graphers["Indicator Radio"] == "Demand by technology")
        & (df_graphers["Mineral Dropdown"] == "Copper")
        & (df_graphers["Type Dropdown"] == "Refinery")
        & (df_graphers["Case Dropdown"] == "Base case")
        & (df_graphers["Demand Scenario Dropdown"] == "Net zero by 2050")
        & (df_graphers["Metric Radio"] == "Total")
    )
    assert len(df_graphers[default_mask]) == 1, "Multiple rows selected for default view."
    df_graphers.loc[default_mask, "defaultView"] = True

    # Prepare explorer metadata.
    config = {
        "explorerTitle": "Minerals Supply and Demand Prospects",
        "explorerSubtitle": "Explore data from the IEA on the future outlook for mineral supply and demand.",
        "entityType": "country or technology",
        # Ensure all entities (countries or technologies) are selected by default, so that stacked area charts are always showing totals.
        "selection": sorted(set(tb["country"])),
    }

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = paths.create_explorer_legacy(config=config, df_graphers=df_graphers)
    ds_explorer.save()

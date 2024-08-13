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
SHARE_OF_GLOBAL_PREFIX = "share_of_global_"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load minerals grapher dataset and read its main table.
    ds = paths.load_dataset("minerals")
    tb = ds.read_table("minerals")

    #
    # Process data.
    #
    # Prepare graphers table of explorer.
    variable_ids = []
    metric_dropdown = []
    commodity_dropdown = []
    sub_commodity_dropdown = []
    share_of_global = []
    map_tab = []
    # Given that USGS current data often has very few data points, we'll specify the minimum year in each view.
    # This way, if there are too few data points, we'll use the latest year as the minimum year, and that way the view
    # will become a bar chart (instead of a line chart with just a few points).
    min_year = []
    for column in tb.drop(columns=["country", "year"]).columns:
        # Select the
        years = tb["year"][tb[column].notnull()]
        if len(years) > 0:
            metric, commodity, sub_commodity, unit = tb[column].metadata.title.split("|")
            if metric.startswith(SHARE_OF_GLOBAL_PREFIX):
                metric = metric.replace(SHARE_OF_GLOBAL_PREFIX, "")
                is_share_of_global = True
            else:
                is_share_of_global = False
            metric = metric.replace("_", " ").capitalize()
            commodity = commodity.capitalize()
            # NOTE: Ideally, we should not show any unit in the subcommodity dropdown. See note below.
            if unit.startswith("tonnes"):
                sub_commodity = f"{sub_commodity.capitalize()} ({unit})"
            else:
                sub_commodity = f"{sub_commodity.capitalize()}"

            # Metric "Unit value" should not have a map tab.
            # Also, imports and exports tend to have very sparse data. For now, remove their map tabs.
            if metric in ["Imports", "Exports", "Unit value"]:
                has_map_tab = False
            else:
                has_map_tab = True

            ############################################################################################################
            # Manually remove the map tab where it is not useful.
            if column in [
                "production_cesium_mine_tonnes",
                "production_chromium_mine_tonnes",
                "production_diamond_mine_and_synthetic__industrial_tonnes",
                "reserves_kyanite_mine__kyanite_and_sillimanite_tonnes",
                "production_soda_ash_synthetic_tonnes",
                "reserves_zeolites_mine_tonnes",
            ]:
                has_map_tab = False
            ############################################################################################################

            # Append extracted values.
            variable_ids.append([f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"])
            metric_dropdown.append(metric)
            commodity_dropdown.append(commodity)
            sub_commodity_dropdown.append(sub_commodity)
            share_of_global.append(is_share_of_global)
            map_tab.append(has_map_tab)

            if (years.max() - years.min()) < 5:
                # If there are only a few data points, show only the latest year (as a bar chart).
                min_year.append(years.max())
            else:
                # Otherwise, show all years (as a line chart).
                min_year.append(years.min())
    df_graphers = pd.DataFrame()
    df_graphers["yVariableIds"] = variable_ids
    df_graphers["Mineral Dropdown"] = commodity_dropdown
    df_graphers["Metric Dropdown"] = metric_dropdown
    df_graphers["Type Dropdown"] = sub_commodity_dropdown
    df_graphers["Share of global Checkbox"] = share_of_global
    df_graphers["minTime"] = min_year
    df_graphers["hasMapTab"] = map_tab

    # Impose that all line charts start at zero.
    df_graphers["yAxisMin"] = 0

    # NOTE: Currently, most columns have "tonnes" as unit, but often there a other units like "tonnes of gross weight".
    # I think that, ideally, all units should be "tonnes" and we should add a footnote to clarify the unit where needed.
    # But at least, for now, remove the "(tonnes)" from the "Type Dropdown" column if all options are in "tonnes".
    # If there are different units within the same dropdown, we keep the brackets, to avoid overwriting.
    remove_unit_mask = df_graphers.groupby(["Mineral Dropdown", "Metric Dropdown"])["Type Dropdown"].transform(
        lambda x: x.str.contains("(tonnes)", regex=False).all()
    )
    df_graphers.loc[remove_unit_mask, "Type Dropdown"] = df_graphers.loc[remove_unit_mask, "Type Dropdown"].str.replace(
        " (tonnes)", ""
    )
    # Warn if there are still mineral-metric-type combinations with multiple units.
    multiple_units = sorted(
        set(df_graphers[df_graphers["Type Dropdown"].str.contains("(", regex=False)]["Mineral Dropdown"])
    )
    if multiple_units:
        log.warning(f"Units different from '(tonnes)' found for {multiple_units}")

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(
            subset=["Mineral Dropdown", "Type Dropdown", "Metric Dropdown", "Share of global Checkbox"], keep=False
        )
    ].empty, error

    # Sort rows conveniently.
    df_graphers["Metric Dropdown"] = pd.Categorical(
        df_graphers["Metric Dropdown"],
        categories=["Production", "Reserves", "Unit value", "Imports", "Exports"],
        ordered=True,
    )
    df_graphers = df_graphers.sort_values(["Mineral Dropdown", "Metric Dropdown", "Type Dropdown"]).reset_index(
        drop=True
    )

    # Choose which indicator to show by default when opening the explorer.
    df_graphers["defaultView"] = False
    df_graphers.loc[
        (df_graphers["Mineral Dropdown"] == "Copper")
        & (df_graphers["Type Dropdown"] == "Mine")
        & (df_graphers["Metric Dropdown"] == "Production")
        & (~df_graphers["Share of global Checkbox"]),
        "defaultView",
    ] = True

    # Prepare explorer metadata.
    config = {
        "explorerTitle": "Minerals",
        "explorerSubtitle": "Explore the amount of minerals that are produced, imported, and exported.",
        "selection": ["World", "Australia", "Chile", "China", "United States"],
    }

    # To begin with, create linear map brackets between 0% and 100% for "share" columns.
    # NOTE: This should be executed only the first time, to have something to start with. Then comment this code, and
    #  continue improving map brackets using the Map Bracketer tool.
    # NOTE: When running these lines, add df_columns as an argument in create_explorer.
    # share_columns = sorted(
    #     set(
    #         sum(
    #             df_graphers[
    #                 (df_graphers["Metric Dropdown"].isin(["Production", "Reserves"]))
    #                 & (df_graphers["Share of global Checkbox"])
    #             ]["yVariableIds"].tolist(),
    #             [],
    #         )
    #     )
    # )
    # df_columns = pd.DataFrame({"catalogPath": share_columns})
    # df_columns["colorScaleNumericBins"] = [
    #     [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0] for column in share_columns
    # ]
    # df_columns["colorScaleScheme"] = "BuGn"

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.write()

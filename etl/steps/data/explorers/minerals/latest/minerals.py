"""Load a grapher dataset and create an explorer dataset with its tsv file."""
import pandas as pd

from etl.helpers import PathFinder, create_explorer

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
    for column in tb.drop(columns=["country", "year"]).columns:
        if tb[column].notnull().any():
            metric, commodity, sub_commodity, unit = tb[column].metadata.title.split("|")
            if metric.startswith(SHARE_OF_GLOBAL_PREFIX):
                metric = metric.replace(SHARE_OF_GLOBAL_PREFIX, "")
                is_share_of_global = True
            else:
                is_share_of_global = False
            metric = metric.replace("_", " ").capitalize()
            commodity = commodity.capitalize()
            sub_commodity = sub_commodity.capitalize()

            # Append extracted values.
            variable_ids.append([f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"])
            metric_dropdown.append(metric)
            commodity_dropdown.append(commodity)
            sub_commodity_dropdown.append(f"{sub_commodity} ({unit})")
            share_of_global.append(is_share_of_global)
    df_graphers = pd.DataFrame()
    df_graphers["yVariableIds"] = variable_ids
    df_graphers["Mineral Dropdown"] = commodity_dropdown
    df_graphers["Metric Dropdown"] = metric_dropdown
    df_graphers["Type Dropdown"] = sub_commodity_dropdown
    df_graphers["Share of global Checkbox"] = share_of_global

    # Add a map tab to all indicators.
    df_graphers["hasMapTab"] = True

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(
            subset=["Mineral Dropdown", "Type Dropdown", "Metric Dropdown", "Share of global Checkbox"], keep=False
        )
    ].empty, error

    # Sort rows conveniently.
    df_graphers = df_graphers.sort_values(["Mineral Dropdown", "Metric Dropdown", "Type Dropdown"]).reset_index(
        drop=True
    )

    # Prepare explorer metadata.
    config = {
        "explorerTitle": "Minerals",
        "explorerSubtitle": "Explore the amount of minerals that are produced, imported, and exported.",
        "selection": ["World", "United States", "China"],
    }

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.save()

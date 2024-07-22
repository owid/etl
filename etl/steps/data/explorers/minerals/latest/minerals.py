"""Load a grapher dataset and create an explorer dataset with its tsv file."""
import pandas as pd

from etl.helpers import PathFinder, create_explorer

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    for column in tb.drop(columns=["country", "year"]).columns:
        if tb[tb[column].notnull()].empty:
            continue
        metric = column.split("_")[0].capitalize()
        commodity = " ".join(column.split("_")[1:]).capitalize()
        variable_ids.append([f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"])
        metric_dropdown.append(metric)
        commodity_dropdown.append(commodity)
    df_graphers = pd.DataFrame()
    df_graphers["yVariableIds"] = variable_ids
    df_graphers["Commodity Dropdown"] = commodity_dropdown
    df_graphers["Metric Dropdown"] = metric_dropdown
    # Add a map tab to all indicators.
    df_graphers["hasMapTab"] = True

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

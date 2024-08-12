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

            # Append extracted values.
            variable_ids.append([f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"])
            metric_dropdown.append(metric)
            commodity_dropdown.append(commodity)
            sub_commodity_dropdown.append(sub_commodity)
            share_of_global.append(is_share_of_global)

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
        "selection": ["World", "United States", "China"],
    }

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.save()

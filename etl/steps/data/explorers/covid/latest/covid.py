"""Load a grapher dataset and create an explorer dataset with its tsv file."""

import pandas as pd

from etl.helpers import PathFinder, create_explorer

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
METRIC_DEATHS = {
    "title": "Confirmed deaths",
    "rows": {
        "new_deaths_per_million_7_day_avg_right": {"interval": "7-day rolling average", "relative": True},
        "total_deaths_per_million": {"interval": "Cumulative", "relative": True},
        "total_deaths": {"interval": "Cumulative", "relative": False},
        "new_deaths_7_day_avg_right": {"interval": "7-day rolling average", "relative": False},
        "new_deaths": {"interval": "New per day", "relative": False},
        "new_deaths_per_million": {"interval": "New per day", "relative": True},
        "weekly_deaths": {"interval": "Weekly", "relative": False},
        "weekly_deaths_per_million": {"interval": "Weekly", "relative": True},
        "biweekly_deaths": {"interval": "Biweekly", "relative": False},
        "biweekly_deaths_per_million": {"interval": "Biweekly", "relative": True},
    },
}
# missing
# - avg_right = smoothed?
# - total_deaths_last12m, total_deaths_last12m_per_million
# - weekly_deaths_change


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load minerals grapher dataset and read its main table.
    ds = paths.load_dataset("cases_deaths")
    tb = ds.read_table("cases_deaths")

    #
    # Process data.
    #
    # Prepare graphers table of explorer.
    variable_ids = []
    dropdown_metric = []
    dropdown_interval = []
    checkbox_relative = []

    metric_name = METRIC_DEATHS["title"]
    for column, params in METRIC_DEATHS["rows"].items():
        # Append extracted values.
        variable_ids.append([f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"])
        dropdown_metric.append(metric_name)
        dropdown_interval.append(params["interval"])
        checkbox_relative.append(params["relative"])

    # Prepare explorer metadata.
    config = {
        "explorerTitle": "COVID-19",
        "explorerSubtitle": "Explore global data on COVID-19.",
        "selection": [
            "United States",
            "Brazil",
            "Japan",
            "Germany",
        ],
        "downloadDataLink": "https://github.com/owid/covid-19-data/tree/master/public/data",
        "subNavId": "coronavirus",
        "subNavCurrentId": "data-explorer-2",
        "wpBlockId": 43869,
        "hasMapTab": "true",
        "yAxisMin": 0,
        "thumbnail": "https://ourworldindata.org/coronavirus-data-explorer.png",
    }

    # Build graphers
    df_graphers = pd.DataFrame()
    df_graphers["yVariableIds"] = variable_ids
    df_graphers["Metric Dropdown"] = dropdown_metric
    df_graphers["Interval Dropdown"] = dropdown_interval
    df_graphers["Relative to Population Checkbox"] = checkbox_relative

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.save()

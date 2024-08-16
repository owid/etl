"""Load a grapher dataset and create an explorer dataset with its tsv file."""

import pandas as pd
import yaml

from etl.helpers import PathFinder, create_explorer

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

OPTION_TYPES = {
    "dropdown": "Dropdown",
    "checkbox": "Checkbox",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load graphers config from YAML
    with open(f"{paths.directory}/covid.graphers.yml", "r") as file:
        graphers = yaml.safe_load(file)

    graphers_config = graphers["config"]
    graphers_views = graphers["views"]

    option_names = [f"{option['name']} {OPTION_TYPES.get(option['type'])}" for option in graphers_config["options"]]

    # Load necessry tables
    # ds = paths.load_dataset("cases_deaths")
    # tb = ds.read_table("cases_deaths")

    # Read all tables
    # tables = {}

    #
    # Process data.
    #
    # Prepare graphers table of explorer.

    records = []
    for view in graphers_views:
        record = {
            "yVariableIds": view["indicator"],
            **{name: option for name, option in zip(option_names, view["options"])},
        }

        records.append(record)

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
    df_graphers = pd.DataFrame.from_records(records)

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.save()

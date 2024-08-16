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
    # Load grapher config from YAML
    with open(f"{paths.directory}/covid.config.yml", "r") as file:
        config = yaml.safe_load(file)
    header = config["config"]
    grapher_views = config["views"]
    grapher_options = config["options"]

    # Load necessry tables
    ds = paths.load_dataset("cases_deaths")
    tb = ds.read_table("cases_deaths")

    # Read all tables
    # tables = {}

    #
    # Process data.
    #
    # Prepare grapher table of explorer.

    records = []
    for view in grapher_views:
        options = bake_options(grapher_options, view["options"])
        var_ids = bake_ids(view["indicator"])
        record = {
            "yVariableIds": var_ids,
            **options,
        }

        # optional

        records.append(record)

    # Build grapher
    df_grapher = pd.DataFrame.from_records(records)

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=header, df_graphers=df_grapher)
    ds_explorer.save()


def bake_options(graphers_options, view_options):
    # inputs:
    # grapher_options, view_options

    dix = {}
    for i, option in enumerate(graphers_options):
        title = f"{option['name']} {OPTION_TYPES.get(option['type'])}"
        if i >= len(view_options):
            if "default" not in option:
                raise Exception(f"Value for option {option['name']} not given, and there is no default!")
            dix[title] = option["default"]
        else:
            dix[title] = view_options[i]
    return dix


def bake_ids(var_ids):
    if isinstance(var_ids, str):
        return [var_ids]
    elif isinstance(var_ids, list):
        return var_ids
    raise TypeError("Variable ID should either be a string or a list of strings.")

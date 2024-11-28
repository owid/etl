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
RELATED = {
    "Confirmed deaths": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "Confirmed cases": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "Cases and deaths": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "Case fatality rate": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "Reproduction rate": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/metrics-explained-covid19-stringency-index",
    },
    "Stringency index": {
        "text": "What is the COVID-19 Stringency Index?",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "Tests": {
        "text": "Data on tests is no longer updated since June 2022",
        "link": "https://ourworldindata.org/covid-testing-data-archived",
    },
    "Tests per case": {
        "text": "Data on tests is no longer updated since June 2022",
        "link": "https://ourworldindata.org/covid-testing-data-archived",
    },
    "Share of positive tests": {
        "text": "Data on tests is no longer updated since June 2022",
        "link": "https://ourworldindata.org/covid-testing-data-archived",
    },
    "Cases, tests, positive and reproduction rate": {
        "text": "Data on tests is no longer updated since June 2022",
        "link": "https://ourworldindata.org/covid-testing-data-archived",
    },
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
    # ds = paths.load_dataset("cases_deaths")
    # tb = ds.read("cases_deaths")

    # Read all tables
    # tables = {}

    #
    # Process data.
    #
    # Prepare grapher table of explorer.

    records = []
    for view in grapher_views:
        # Get options and variable IDs
        options = bake_options(grapher_options, view["options"])
        var_ids = bake_ids(view["indicator"])

        record = {
            "yVariableIds": var_ids,
            **options,
        }

        # Tweak view
        name = view["options"][0]
        if name in RELATED:
            view["relatedQuestionText"] = RELATED[name]["text"]
            view["relatedQuestionUrl"] = RELATED[name]["link"]

        # optional
        fields_optional = [
            "title",
            "subtitle",
            "type",
            "hasMapTab",
            "hideAnnotationFieldsInTitle",
            "sortBy",
            "sortColumnSlug",
            "hideTotalValueLabel",
            "selectedFacetStrategy",
            "facetYDomain",
            "timelineMinTime",
            "note",
            "defaultView",
            "relatedQuestionText",
            "relatedQuestionUrl",
            "tab",
        ]
        for field in fields_optional:
            if field in view:
                if isinstance(view[field], bool):
                    v = str(view[field]).lower()
                    record[field] = v
                else:
                    record[field] = view[field]

        # Add record
        records.append(record)

    # Build grapher
    df_grapher = pd.DataFrame.from_records(records)

    # Set defaults
    field_defaults = {
        "hideAnnotationFieldsInTitle": "true",
        "hasMapTab": "true",
    }
    for field, default in field_defaults.items():
        if field in df_grapher.columns:
            df_grapher[field] = df_grapher[field].fillna(default)
        else:
            df_grapher[field] = default

    # Set dtypes
    df_grapher = df_grapher.astype(
        {
            "timelineMinTime": "Int64",
        }
    )
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

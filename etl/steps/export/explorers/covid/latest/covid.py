"""Load a grapher dataset and create an explorer dataset with its tsv file."""

import pandas as pd

from etl.collections.utils import (
    expand_catalog_paths,
    get_indicators_in_view,
    get_tables_by_name_mapping,
    records_to_dictionary,
)
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
    config = paths.load_explorer_config()

    header = config["config"]
    grapher_views = config["views"]
    grapher_dimensions = config["dimensions"]

    # Load necessary tables
    # ds = paths.load_dataset("cases_deaths")
    # tb = ds.read("cases_deaths")

    # Read all tables
    # tables = {}

    #
    # Process data.
    #
    # 1. Obtain `dimensions_display` dictionary. This helps later when remixing the Explorer configuration.
    # 2. Obtain `tables_by_name`: This helps in expanding the indicator paths if incomplete (e.g. table_name#short_name -> complete URI based on dependencies).
    # 3. Obtain `df_grapher`: This is the final DataFrame that will be saved as the Explorer dataset. It is basically a different presentation of the config

    # 1. Prepare Dimension display dictionary
    dimensions_display = records_to_dictionary(grapher_dimensions, key="slug")
    for slug, values in dimensions_display.items():
        # Sanity checks
        assert "name" in values, f"name not found for dimension: {slug}!"
        assert "presentation" in values, f"presentation not found for dimension: {slug}!"
        assert "type" in values["presentation"], f"type not found for dimension: {slug}!"

        # Index choices
        if "choices" not in values:
            assert values["presentation"]["type"] == "checkbox", f"Choices not found for dimension: {slug}!"
        else:
            values["choices"] = records_to_dictionary(values["choices"], key="slug")

        # Widget name
        values["widget_name"] = f"{values['name']} {values['presentation']['type'].title()}"

    # 2. Get table information by table name, and table URI
    tables_by_name = get_tables_by_name_mapping(paths.dependencies)

    # 3. Remix configuration to generate explorer-friendly graphers table.
    records = []
    for view in grapher_views:
        # Expand catalog paths
        expand_catalog_paths(view, tables_by_name)

        # Build dimensions dictionary for a view
        dimensions = bake_dimensions_view(
            dimensions_display=dimensions_display,
            view=view,
        )
        # Get options and variable IDs
        indicator_paths = get_indicators_in_view(view)

        # Build record
        record = {
            **dimensions,
        }
        y = [v["path"] for v in indicator_paths if v["dimension"] == "y"]
        x = [v["path"] for v in indicator_paths if v["dimension"] == "x"]
        # size = [v["path"] for v in var_ids if v["dimension"] == "size"]
        # color = [v["path"] for v in var_ids if v["dimension"] == "color"]
        if y:
            record["yVariableIds"] = y
        if x:
            record["xVariableIds"] = x

        # TODO: which names do these use?
        # if size:
        #     record["sizeVariableIds"] = size
        # if color:
        #     record["colorVariableIds"] = color

        # Tweak view
        name = view["dimensions"]["metric"]
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
        if "config" in view:
            for field in fields_optional:
                if field in view["config"]:
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
    if "timelineMinTime" in df_grapher.columns:
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


def bake_dimensions_view(dimensions_display, view):
    """Prepare view config for Explorer.

    Given is dimension_slug: choice_slug. We need to convert it to dimension_name: choice_name (using dimensions_display).
    """
    view_dimensions = {}
    for slug_dim, slug_choice in view["dimensions"].items():
        if "choices" in dimensions_display[slug_dim]:
            view_dimensions[dimensions_display[slug_dim]["widget_name"]] = dimensions_display[slug_dim]["choices"][
                slug_choice
            ]["name"]
        else:
            view_dimensions[dimensions_display[slug_dim]["widget_name"]] = slug_choice
    return view_dimensions


def bake_ids(view):
    """Prepare variable IDs for Explorer."""
    indicators_y = view["indicators"]["y"]
    if isinstance(indicators_y, str):
        indicators_y = [indicators_y]
    return [var_id["variableId"] for var_id in var_ids]

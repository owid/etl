"""Load a grapher dataset and create an explorer dataset with its tsv file.

This step contains tooling that should moved to a more general module so that it can be easily used by other explorer steps!

# optional
        fields_optional = [
            "title",
            "subtitle",
            "type",  # NO
            "hasMapTab",
            "hideAnnotationFieldsInTitle",
            "sortBy",
            "sortColumnSlug",
            "hideTotalValueLabel",
            "selectedFacetStrategy",
            "facetYDomain",  # NO
            "timelineMinTime",
            "note",
            "defaultView",  # NO
            "relatedQuestionText",  # NO
            "relatedQuestionUrl",  # NO
            "tab",
        ]
"""

from etl.collections.explorers import create_explorer, process_views
from etl.collections.model import Explorer
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

RELATED = {
    "deaths": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "cases": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "cases_deaths": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "cfr": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "r": {
        "text": "Since 8 March, we rely on data from the WHO for confirmed cases and deaths",
        "link": "https://ourworldindata.org/metrics-explained-covid19-stringency-index",
    },
    "stringency": {
        "text": "What is the COVID-19 Stringency Index?",
        "link": "https://ourworldindata.org/covid-jhu-who",
    },
    "tests": {
        "text": "Data on tests is no longer updated since June 2022",
        "link": "https://ourworldindata.org/covid-testing-data-archived",
    },
    "tests_cases": {
        "text": "Data on tests is no longer updated since June 2022",
        "link": "https://ourworldindata.org/covid-testing-data-archived",
    },
    "tests_pos": {
        "text": "Data on tests is no longer updated since June 2022",
        "link": "https://ourworldindata.org/covid-testing-data-archived",
    },
    "cases_tests_pos_r": {
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

    explorer = Explorer.from_dict(config)
    process_views(explorer, paths.dependencies)

    ds_explorer = create_explorer(
        dest_dir=dest_dir,
        config=config,
        paths=paths,
    )

    #
    # Process data.
    #

    # Set defaults
    # field_defaults = {
    #     "hideAnnotationFieldsInTitle": "true",
    #     "hasMapTab": "true",
    # }
    # for field, default in field_defaults.items():
    #     if field in df_grapher.columns:
    #         df_grapher[field] = df_grapher[field].fillna(default)
    #     else:
    #         df_grapher[field] = default

    # Set dtypes -- TODO: is this needed?
    # if "timelineMinTime" in df_grapher.columns:
    #     df_grapher = df_grapher.astype(
    #         {
    #             "timelineMinTime": "Int64",
    #         }
    #     )
    #
    # Save outputs.
    #

    # Create a new explorers dataset and tsv file.
    ds_explorer.save()

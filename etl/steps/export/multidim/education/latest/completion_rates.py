"""Load a meadow dataset and create a garden dataset."""

from etl.collection import combine_collections
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "tab": "map",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "change-country",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load datasets

    # Load grapher dataset.
    ds = paths.load_dataset("education_sdgs")
    tb = ds.read("education_sdgs", load_data=False)

    cols = tb.filter(regex=r"(completion_cols)__sex_").columns
    print(tb)
    tb = adjust_dimensions_enrolment(tb)

    c = paths.create_collection(
        config=config,
        tb=[tb],
        common_view_config=MULTIDIM_CONFIG,
    )
    # Add grouped view
    c.group_views(
        groups=[
            {
                "dimension": "sex",
                "choice_new_slug": "sex_side_by_side",
                "choices": ["girls", "boys"],
                "view_config": {
                    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
                    "originUrl": "ourworldindata.org/education",
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "change-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "selectedFacetStrategy": "entity",
                    "yAxis": {"min": 0, "max": 100},
                },
            },
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "view_config": {
                    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
                    "originUrl": "ourworldindata.org/education",
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "change-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "selectedFacetStrategy": "entity",
                    "yAxis": {"min": 0, "max": 100},
                },
            },
        ]
    )
    #
    # Save garden dataset.
    #
    c.save()


def adjust_dimensions_enrolment(tb):
    """
    Add dimensions to completion table columns.

    """

    # Helper maps for pattern matching
    level_keywords = {
        "pre_primary": "preprimary",
        "pre_enrr": "preprimary",
        "primary": "primary",
        "lower_secondary": "lower_secondary",
        "upper_secondary": "upper_secondary",
    }

    sex_keywords = {"both_sexes": "both", "male": "boys", "female": "girls"}

    cols_to_add_dimensions = [col for col in tb.columns if col not in ["country", "year"]]

    # Iterate and set dimensions directly
    for col in cols_to_add_dimensions:
        tb[col].metadata.original_short_name = "completion_rates"
        tb[col].metadata.dimensions = {}

        # --- Level ---
        level = None
        for key, val in level_keywords.items():
            if key in col:
                level = val
                break
        # --- Sex ---
        sex = None
        for key, val in sex_keywords.items():
            if f"__{key}__" in col or col.endswith(f"_{key}"):
                sex = val
                break

        # Set dimensions
        tb[col].metadata.dimensions["level"] = level
        tb[col].metadata.dimensions["sex"] = sex or "both"

    # Add dimension definitions at table level
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.extend(
            [
                {"name": "Education level", "slug": "level"},
                {"name": "Gender", "slug": "sex"},
            ]
        )

    return tb

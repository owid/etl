"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "hasMapTab": True,
    "tab": "map",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "change-country",
    "yAxis": {"min": 0, "max": 100},
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("education_sdgs")
    tb = ds.read("education_sdgs", load_data=False)

    # Match full column names that contain all required parts
    both_sexes = [
        col for col in tb.columns if "completion_rate" in col and "both_sexes" in col and "modelled_data" in col
    ]
    # Match full column names that contain all required parts
    female = [
        col
        for col in tb.columns
        if "completion_rate" in col and "education__female__pct__" in col and "modelled_data" in col
    ]
    male = [
        col
        for col in tb.columns
        if "completion_rate" in col and "education__male__pct__" in col and "modelled_data" in col
    ]

    completion_cols = both_sexes + female + male
    # Select only relevant columns
    tb = tb.loc[:, ["country", "year"] + list(completion_cols)].copy()

    # Adjust dimensions
    tb = adjust_dimensions(tb)

    c = paths.create_collection(
        config=config,
        tb=tb,
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

    for view in c.views:
        # Update title and subtitle based on view dimensions
        sex = view.dimensions.get("sex")
        level = view.dimensions.get("level")

        # Create a copy of the config to avoid shared references
        if view.config is None:
            view.config = {}
        else:
            view.config = view.config.copy()

        # Generate dynamic title
        if sex and level:
            view.config["title"] = generate_title_by_gender_and_level(sex, level)

        # Generate dynamic subtitle
        if level:
            view.config["subtitle"] = generate_subtitle_by_level(level, sex)

        edit_indicator_displays(view)
    #
    # Save garden dataset.
    #
    c.save()


def adjust_dimensions(tb):
    """
    Add dimensions to completion rates table.
    """

    # Helper maps for pattern matching
    level_keywords = {
        "primary": "primary",
        "lower_secondary": "lower_secondary",
        "upper_secondary": "upper_secondary",
    }

    sex_keywords = {"both_sexes": "both", "m": "boys", "f": "girls"}

    # Iterate and set dimensions directly
    for col in tb.columns:
        if col in ["country", "year"]:
            continue
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

    # Add our dimension definitions
    tb.metadata.dimensions.extend(
        [
            {"name": "Education level", "slug": "level"},
            {"name": "Gender", "slug": "sex"},
        ]
    )

    return tb


# Common mappings used by both title and subtitle functions
GENDER_MAPPINGS = {
    "title": {"both": "children", "boys": "boys", "girls": "girls", "sex_side_by_side": "boys and girls"},
    "subtitle": {"both": "children", "boys": "boys", "girls": "girls", "sex_side_by_side": "boys and girls"},
}

LEVEL_MAPPINGS = {
    "title": {
        "primary": "primary school",
        "lower_secondary": "lower secondary school",
        "upper_secondary": "upper secondary school",
        "level_side_by_side": "school",
    },
    "subtitle": {
        "primary": "[primary](#dod:primary-education)",
        "lower_secondary": "[lower secondary](#dod:lower-secondary-education)",
        "upper_secondary": "[upper secondary](#dod:upper-secondary-education)",
        "level_side_by_side": "[primary](#dod:primary-education), [lower secondary](#dod:lower-secondary-education), and [upper secondary](#dod:upper-secondary-education)",
    },
}


def _get_gender_term(sex, context="title"):
    """Get appropriate gender term based on context."""
    return GENDER_MAPPINGS[context].get(sex, "")


def generate_title_by_gender_and_level(sex, level):
    """Generate title based on gender and education level."""
    gender_term = _get_gender_term(sex, "title")
    level_term = LEVEL_MAPPINGS["title"].get(level, "")

    if not level_term:
        raise ValueError(f"Unknown education level: {level}")

    if level == "level_side_by_side":
        return f"Share of {gender_term} completing school, by education level"
    else:
        return f"Share of {gender_term} completing {level_term}"


def generate_subtitle_by_level(level, sex):
    """Generate subtitle based on education level and gender with links."""
    level_term = LEVEL_MAPPINGS["subtitle"].get(level, "")
    gender_term = _get_gender_term(sex, "subtitle")

    if not level_term:
        raise ValueError(f"Unknown education level: {level}")

    if level == "level_side_by_side" or sex == "sex_side_by_side":
        return f" Percentage of boys of {gender_term} aged 3 to 5 years above the official age for the last grade of {level_term} education who have successfully completed that level."


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.dimensions.get("level") == "level_side_by_side":
        if view.indicators.y is not None:
            for indicator in view.indicators.y:
                display_name = None

                if "primary" in indicator.catalogPath:
                    display_name = "Primary"
                elif "lower_secondary" in indicator.catalogPath:
                    display_name = "Lower secondary"
                elif "upper_secondary" in indicator.catalogPath:
                    display_name = "Upper secondary"

                if display_name:
                    indicator.display = {
                        "name": display_name,
                    }

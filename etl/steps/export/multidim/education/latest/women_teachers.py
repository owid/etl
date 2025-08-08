"""Create multidimensional collection for share of women teachers data."""

import re

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# --------------------- #
#   Constants & Config  #
# --------------------- #

ID_COLUMNS = ["country", "year"]

WOMEN_TEACHER_PATTERN = r"percentage_of_teachers_in_(primary|secondary|tertiary)_education_who_are_female__pct"

# Main chart configuration
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "hasMapTab": True,
    "tab": "map",
    "addCountryMode": "add-country",
}

# Grouped view chart configuration
GROUPED_VIEW_CONFIG = MULTIDIM_CONFIG | {
    "hasMapTab": False,
    "tab": "chart",
    "yAxis": {"min": 0, "max": 100, "facetDomain": "independent"},
    "selectedFacetStrategy": "entity",
    "addCountryMode": "change-country",
}

# --------------------- #
#      Dimensions       #
# --------------------- #

EDUCATION_LEVELS = {
    "primary": {
        "display_name": "Primary education",
        "title_term": "primary education",
        "age_range": "primary education (typically ages 6-11)",
    },
    "secondary": {
        "display_name": "Secondary education",
        "title_term": "secondary education",
        "age_range": "secondary education (typically ages 12-17)",
    },
    "tertiary": {
        "display_name": "Tertiary education",
        "title_term": "tertiary education",
        "age_range": "tertiary education (higher education)",
    },
}


def run() -> None:
    """Main function to process women teachers data and create collection."""
    config = paths.load_collection_config()

    ds = paths.load_dataset("education_opri")
    tb = ds.read("education_opri", load_data=False)

    women_teacher_cols = get_women_teacher_columns(tb)
    tb = tb.loc[:, ID_COLUMNS + women_teacher_cols].copy()
    tb = adjust_dimensions(tb)

    collection = paths.create_collection(
        config=config,
        tb=[tb],
        common_view_config=MULTIDIM_CONFIG,
    )

    create_grouped_views(collection)

    for view in collection.views:
        edit_indicator_displays(view)

    collection.save()


def get_women_teacher_columns(tb):
    """Filter women teacher columns."""
    return [col for col in tb.columns if re.search(WOMEN_TEACHER_PATTERN, col, re.IGNORECASE)]


def adjust_dimensions(tb):
    """Add dimensions to women teacher data columns."""

    def extract_level(col):
        if "primary" in col.lower():
            return "primary"
        elif "secondary" in col.lower():
            return "secondary"
        elif "tertiary" in col.lower():
            return "tertiary"
        return "primary"  # Default fallback

    for col in tb.columns:
        if col in ID_COLUMNS:
            continue

        tb[col].metadata.original_short_name = "women_teachers"
        level = extract_level(col)

        tb[col].metadata.dimensions = {
            "level": level,
        }

    tb.metadata.dimensions.extend(
        [
            {"name": "Education level", "slug": "level"},
        ]
    )

    return tb


def create_grouped_views(collection):
    """Add grouped views for education level comparisons."""

    def get_view_metadata(view):
        """Generate view metadata with actual title and subtitle."""
        return {
            "presentation": {"title_public": generate_title_by_dimensions(view)},
            "description_short": generate_subtitle_by_dimensions(view),
        }

    def get_view_config(view):
        """Generate chart configuration for grouped views."""
        return GROUPED_VIEW_CONFIG | {
            "title": generate_title_by_dimensions(view),
            "subtitle": generate_subtitle_by_dimensions(view),
        }

    collection.group_views(
        groups=[
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "choices": ["primary", "secondary", "tertiary"],
                "view_config": get_view_config,
                "view_metadata": get_view_metadata,
            },
        ],
    )


def generate_title_by_dimensions(view):
    """Generate chart title for grouped views."""
    if view.matches(level="level_side_by_side"):
        return "Share of teachers who are women, by education level"


def generate_subtitle_by_dimensions(view):
    """Generate descriptive subtitle for grouped views."""
    if view.matches(level="level_side_by_side"):
        return "Percentage of teachers who are women across [primary](#dod:primary-education), [secondary](#dod:secondary-education), and [tertiary](#dod:tertiary-education) education levels, shown as a percentage of all teachers."


def edit_indicator_displays(view):
    """Clean up indicator display names for grouped views."""

    level_display = {
        "primary": "Primary",
        "secondary": "Secondary",
        "tertiary": "Tertiary",
    }

    for ind in view.indicators.y:
        if view.matches(level="level_side_by_side"):
            for k, v in level_display.items():
                if k in ind.catalogPath:
                    ind.display = {"name": v}
                    break

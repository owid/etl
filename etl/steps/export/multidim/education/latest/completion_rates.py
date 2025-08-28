"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Detailed colors
COLOR_PRIMARY = "#4C6A9C"
COLOR_LOWER_SECONDARY = "#883039"
COLOR_UPPER_SECONDARY = "#578145"
COLOR_TERTIARY = "#B16214"

COLOR_BOYS = "#00847E"
COLOR_GIRLS = "#E56E5A"

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "yAxis": {"min": 0, "max": 100},
    "hasMapTab": True,
    "tab": "map",
    "addCountryMode": "add-country",
}

# Common grouped view configuration
GROUPED_VIEW_CONFIG = MULTIDIM_CONFIG | {
    "hasMapTab": False,
    "tab": "chart",
    "selectedFacetStrategy": "entity",
}

# Column filtering patterns
COMPLETION_RATE_PATTERNS = {
    "both_sexes": ["completion_rate", "both_sexes", "modelled_data"],
    "female": ["completion_rate", "education__female__pct__", "modelled_data"],
    "male": ["completion_rate", "education__male__pct__", "modelled_data"],
}


def run() -> None:
    """Main function to process completion rates data and create collection."""
    # Load inputs
    config = paths.load_collection_config()
    ds = paths.load_dataset("education_sdgs")
    tb = ds.read("education_sdgs", load_data=False)

    # Filter completion rate columns
    completion_cols = get_completion_rate_columns(tb)

    # Select only relevant columns
    tb = tb.loc[:, ["country", "year"] + completion_cols].copy()

    # Adjust dimensions
    tb = adjust_dimensions(tb)

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=tb,
        common_view_config=MULTIDIM_CONFIG,
    )

    # Add grouped views
    create_grouped_views(c)

    # Edit FAUST
    c.set_global_config(
        config={
            "title": lambda view: generate_title_by_gender_and_level(view),
        }
    )

    # Edit display names
    for view in c.views:
        # Update title and subtitle based on view dimensions
        sex = view.dimensions["sex"]
        level = view.dimensions["level"]
        if sex == "sex_side_by_side" or level == "level_side_by_side":
            view.metadata = {
                "description_from_producer": "",
                "description_short": view.config["subtitle"],
                "presentation": {
                    "title_public": view.config["title"],
                },
            }
        else:
            # Only updated description_short for other views
            view.metadata = {
                "presentation": {
                    "title_public": view.config["title"],
                },
            }
        edit_indicator_displays(view)

    # Save collection
    c.save()


def get_completion_rate_columns(tb):
    """Filter completion rate columns by gender category."""
    completion_cols = []

    for gender, patterns in COMPLETION_RATE_PATTERNS.items():
        cols = [col for col in tb.columns if all(pattern in col for pattern in patterns)]
        completion_cols.extend(cols)

    return completion_cols


def adjust_dimensions(tb):
    """Add dimensions to completion rates table columns."""

    # Auxiliary functions just used in this function
    def _extract_dimension(column_name, keyword_map):
        """Extract dimension value from column name using keyword mapping."""
        for keyword, value in keyword_map.items():
            if keyword in column_name:
                return value
        return None

    def _extract_gender(column_name, sex_keywords):
        """Extract gender dimension from column name."""
        for keyword, value in sex_keywords.items():
            if f"__{keyword}__" in column_name or column_name.endswith(f"_{keyword}"):
                return value
        return "both"  # Default for both_sexes

    # Dimension mapping configurations
    LEVEL_KEYWORDS = {
        "primary": "primary",
        "lower_secondary": "lower_secondary",
        "upper_secondary": "upper_secondary",
    }

    SEX_KEYWORDS = {"both_sexes": "both", "m": "boys", "f": "girls"}

    # Process each column
    for col in tb.columns:
        if col in ["country", "year"]:
            continue
        # Extract education level
        level = _extract_dimension(col, LEVEL_KEYWORDS)

        # Extract gender
        sex = _extract_gender(col, SEX_KEYWORDS)
        # Set indicator name
        tb[col].metadata.original_short_name = "completion_rates"
        # Set dimensions
        tb[col].metadata.dimensions = {
            "level": level,
            "sex": sex or "both",  # fallback for non-disaggregated vars
        }

    # Add dimension definitions to table metadata
    tb.metadata.dimensions.extend(
        [
            {"name": "Education level", "slug": "level"},
            {"name": "Gender", "slug": "sex"},
        ]
    )

    return tb


def create_grouped_views(collection):
    """Add grouped views for gender and education level comparisons."""
    view_metadata = {
        "presentation": {
            "title_public": "{title}",
        },
        "description_short": "{subtitle}",
    }
    view_config = GROUPED_VIEW_CONFIG | {
        "title": "{title}",
        "subtitle": "{subtitle}",
    }

    collection.group_views(
        groups=[
            {
                "dimension": "sex",
                "choice_new_slug": "sex_side_by_side",
                "choices": ["girls", "boys"],
                "view_config": view_config
                | {"map": {"colorScale": {"customCategoryColors": {"Girls": COLOR_GIRLS, "Boys": COLOR_BOYS}}}},
                "view_metadata": view_metadata,
            },
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "view_config": view_config
                | {
                    "map": {
                        "colorScale": {
                            "customCategoryColors": {
                                "Primary": COLOR_PRIMARY,
                                "Lower secondary": COLOR_LOWER_SECONDARY,
                                "Upper secondary": COLOR_UPPER_SECONDARY,
                            }
                        }
                    }
                },
                "view_metadata": view_metadata,
            },
        ],
        params={
            "title": lambda view: generate_title_by_gender_and_level(view),
            "subtitle": lambda view: generate_subtitle_by_level(view),
        },
    )


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


def generate_title_by_gender_and_level(view):
    """Generate title based on gender and education level."""
    sex, level = view.dimensions["sex"], view.dimensions["level"]

    # Get gender term
    gender_term = GENDER_MAPPINGS["title"].get(sex, "")
    # Get level term
    level_term = LEVEL_MAPPINGS["title"].get(level, "")

    if not level_term:
        raise ValueError(f"Unknown education level: {level}")

    if level == "level_side_by_side":
        return f"Share of {gender_term} that have completed school, by education level"
    else:
        return f"Share of {gender_term} that have completed {level_term}"


def generate_subtitle_by_level(view):
    """Generate subtitle based on education level and gender with links."""
    sex, level = view.dimensions["sex"], view.dimensions["level"]

    level_term = LEVEL_MAPPINGS["subtitle"].get(level, "")
    gender_term = GENDER_MAPPINGS["subtitle"].get(sex, "")

    if not level_term:
        raise ValueError(f"Unknown education level: {level}")

    return f"The share of {gender_term} who are three to five years older than the official age for the last grade of {level_term} education who have successfully completed it. This broader age band is used to include children who started school late or had to resit particular years."


def edit_indicator_displays(view):
    """Edit display names and colors for the grouped views."""

    # Handle level side-by-side views (education levels)
    if view.matches(level="level_side_by_side"):
        # Display name and color mappings for education levels
        LEVEL_CONFIG = {
            "primary": {"name": "Primary", "color": COLOR_PRIMARY},
            "lower_secondary": {"name": "Lower secondary", "color": COLOR_LOWER_SECONDARY},
            "upper_secondary": {"name": "Upper secondary", "color": COLOR_UPPER_SECONDARY},
        }

        for indicator in view.indicators.y:
            for level_key, config in LEVEL_CONFIG.items():
                if level_key in indicator.catalogPath:
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

    # Handle sex side-by-side views (gender)
    elif view.matches(sex="sex_side_by_side"):
        # Display name and color mappings for gender
        GENDER_CONFIG = {
            "female": {"name": "Girls", "color": COLOR_GIRLS},
            "male": {"name": "Boys", "color": COLOR_BOYS},
        }

        for indicator in view.indicators.y:
            for gender_key, config in GENDER_CONFIG.items():
                if f"__{gender_key}__" in indicator.catalogPath:
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

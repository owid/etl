"""Create multidimensional collection for qualified and trained teachers data.

This module processes teacher qualification data from two sources:
- UNESCO (education_sdgs): Academic qualifications of teachers
- World Bank WDI: Pedagogical training of teachers

It creates a multidimensional collection with grouped views comparing:
1. Different education levels (side-by-side)
2. Qualified vs trained teachers (side-by-side)
"""

import re

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# --------------------- #
#   Constants & Config  #
# --------------------- #
# Color constants for education levels and gender
COLOR_PREPRIMARY = "#D73C50"
COLOR_PRIMARY = "#4C6A9C"
COLOR_LOWER_SECONDARY = "#883039"
COLOR_UPPER_SECONDARY = "#578145"

COLOR_QUALIFIED = "#00847E"
COLOR_TRAINED = "#E56E5A"
# Standard columns present in all datasets
ID_COLUMNS = ["country", "year"]

# Regex patterns to identify teacher-related columns from different data sources
QUALIFIED_TEACHER_PATTERN = (
    r"percentage_of_qualified_teachers_in_(pre_primary|primary|lower_secondary|upper_secondary)_education__both_sexes"
)
TRAINED_TEACHER_PATTERN = r"^(se_pre_tcaq_zs|se_prm_tcaq_zs|se_sec_tcaq_lo_zs|se_sec_tcaq_up_zs)$"

# Main chart configuration for individual views (maps)
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "hasMapTab": True,
    "tab": "map",
    "addCountryMode": "add-country",
}

# Configuration for grouped views (side-by-side comparisons)
GROUPED_VIEW_CONFIG = MULTIDIM_CONFIG | {
    "hasMapTab": False,  # No map tab for grouped views
    "tab": "chart",  # Default to chart view
    "yAxis": {"min": 0, "max": 100, "facetDomain": "independent"},  # Percentage scale
    "selectedFacetStrategy": "entity",  # Allow entity selection
    "addCountryMode": "add-country",  # Allow adding countries for easier comparison
}

# --------------------- #
#      Dimensions       #
# --------------------- #

# Education level configurations with display names, WDI codes, and DOD links
EDUCATION_LEVELS = {
    "pre_primary": {
        "keywords": ["pre", "preprimary"],  # Keywords for column matching
        "wdi_code": "se_pre_tcaq_zs",  # World Bank indicator code
        "display_name": "Pre-primary",  # Short display name for charts
        "title_term": "pre-primary education",  # Full term for titles
        "subtitle_link": "[pre-primary](#dod:pre-primary-education)",  # DOD link for subtitles
    },
    "primary": {
        "keywords": ["prm", "primary"],
        "wdi_code": "se_prm_tcaq_zs",
        "display_name": "Primary",
        "title_term": "primary education",
        "subtitle_link": "[primary](#dod:primary-education)",
    },
    "lower_secondary": {
        "keywords": ["sec", "lower", "lo"],
        "wdi_code": "se_sec_tcaq_lo_zs",
        "display_name": "Lower secondary",
        "title_term": "lower secondary education",
        "subtitle_link": "[lower secondary](#dod:lower-secondary-education)",
    },
    "upper_secondary": {
        "keywords": ["sec", "upper", "up"],
        "wdi_code": "se_sec_tcaq_up_zs",
        "display_name": "Upper secondary",
        "title_term": "upper secondary education",
        "subtitle_link": "[upper secondary](#dod:upper-secondary-education)",
    },
}

# Teacher type configurations for qualified vs trained teachers
TEACHER_TYPES = {
    "qualified": {
        "title": "Qualified teachers",  # Display title
        "description": "academic qualifications",  # Short description for subtitles
        "full_description": "who meet the minimum academic qualifications required in the subject they teach at that level in a given country",
        "catalog_key": "qualified",  # Key to match in catalog paths
    },
    "trained": {
        "title": "Trained teachers",
        "description": "pedagogical training",
        "full_description": "who have received at least the minimum organized pedagogical training required for teaching at that level",
        "catalog_key": "tcaq",  # WDI uses "tcaq" in column names
    },
}

# Detailed description explaining the difference between qualified and trained teachers
# Used in teacher type side-by-side comparison views
TEACHER_TYPE_DESCRIPTION_KEY = [
    "Teachers need both subject knowledge and teaching skills to be effective. These indicators measure different aspects of teacher preparation.",
    "**Qualified teachers** have the minimum academic qualifications required for teaching their subjects - university degrees, subject certifications, or other credentials that demonstrate mastery of content knowledge.",
    "**Trained teachers** have completed the minimum organized pedagogical training required for teaching - pre-service education programs, in-service training, or certification focusing on teaching methods and classroom management.",
    "Both indicators show the percentage of teachers meeting their country's minimum standards. Values below 100% suggest some teachers lack the preparation their country considers necessary.",
    "Cross-country comparisons should be interpreted carefully since requirements vary significantly between nations.",
    "Data comes from administrative records at schools and learning centers, including information on teacher credentials and employment status.",
]


def run() -> None:
    """Main function to process teachers data and create collection."""
    config = paths.load_collection_config()
    tbs_adjusted = []

    # Process both UNESCO (qualified) and World Bank (trained) teacher datasets
    for dataset_name in ["education_sdgs", "wdi"]:
        ds = paths.load_dataset(dataset_name)
        tb = ds.read(dataset_name, load_data=False)

        # Filter to only teacher-related columns for this dataset
        teacher_cols = get_teacher_columns(tb, dataset_name)
        if teacher_cols:
            # Keep only ID columns and teacher columns
            tb = tb.loc[:, ID_COLUMNS + teacher_cols].copy()
            # Add dimensional metadata for multidimensional views
            tb = adjust_dimensions(tb, dataset_name)
            tbs_adjusted.append(tb)

    # Create the base multidimensional collection
    collection = paths.create_collection(
        config=config,
        tb=tbs_adjusted,
        common_view_config=MULTIDIM_CONFIG,
    )

    # Add grouped views for side-by-side comparisons
    create_grouped_views(collection)

    # Clean up indicator display names for better chart labels and set view metadata
    for view in collection.views:
        level = view.dimensions["level"]
        teacher_type = view.dimensions["teacher_type"]
        if level == "level_side_by_side" or teacher_type == "teacher_type_side_by_side":
            view.metadata = {
                "description_from_producer": "",
                "description_short": view.config["subtitle"],
                "presentation": {
                    "title_public": view.config["title"],
                },
            }

        edit_indicator_displays(view)

    collection.save()


def get_teacher_columns(tb, dataset_name):
    """Filter teacher columns based on dataset source.

    Args:
        tb: Table containing all columns from the dataset
        dataset_name: Either 'education_sdgs' (qualified teachers) or 'wdi' (trained teachers)

    Returns:
        List of column names that match teacher-related patterns for the given dataset
    """
    # Map each dataset to its specific column pattern
    patterns = {
        "education_sdgs": QUALIFIED_TEACHER_PATTERN,  # UNESCO qualified teacher columns
        "wdi": TRAINED_TEACHER_PATTERN,  # World Bank trained teacher columns
    }
    pattern = patterns.get(dataset_name)
    return [col for col in tb.columns if pattern and re.search(pattern, col, re.IGNORECASE)]


def adjust_dimensions(tb, dataset_name):
    """Add dimensional metadata to teacher data columns for multidimensional views.

    This adds two dimensions to each indicator:
    - level: Education level (pre_primary, primary, lower_secondary, upper_secondary)
    - teacher_type: Type of teacher qualification (qualified, trained)

    Args:
        tb: Table with teacher data columns
        dataset_name: Source dataset ('education_sdgs' or 'wdi')

    Returns:
        Table with dimensional metadata added
    """

    def extract_level(col, dataset_name):
        """Extract education level from column name based on dataset type."""
        if dataset_name == "wdi":
            # WDI uses standardized codes - match against known WDI codes
            for level, config in EDUCATION_LEVELS.items():
                if col.lower() == config["wdi_code"].lower():
                    return level
        else:  # education_sdgs
            # UNESCO uses descriptive names - parse education level from column text
            col_lower = col.lower()
            if "pre_primary" in col_lower:
                return "pre_primary"
            elif "primary" in col_lower:
                return "primary"
            elif "lower_secondary" in col_lower:
                return "lower_secondary"
            elif "upper_secondary" in col_lower:
                return "upper_secondary"

    # Map dataset source to teacher type dimension
    teacher_type_mapping = {"education_sdgs": "qualified", "wdi": "trained"}
    teacher_type = teacher_type_mapping.get(dataset_name, "qualified")

    # Add dimensions to each indicator column
    for col in tb.columns:
        if col in ID_COLUMNS:
            continue

        # Set common metadata
        tb[col].metadata.original_short_name = "teachers_qualified_trained"

        # Add dimensional metadata for multidimensional collection
        tb[col].metadata.dimensions = {
            "level": extract_level(col, dataset_name),
            "teacher_type": teacher_type,
        }

    # Register dimensions with the table metadata
    tb.metadata.dimensions.extend(
        [
            {"name": "Education level", "slug": "level"},
            {"name": "Teacher type", "slug": "teacher_type"},
        ]
    )

    return tb


def create_grouped_views(collection):
    """Add grouped views for side-by-side comparisons.

    Creates two types of grouped views:
    1. Teacher type comparison: Qualified vs Trained teachers for a specific education level
    2. Education level comparison: One teacher type across all education levels
    """

    def get_view_metadata(view, include_description_key=False):
        """Generate view metadata with optional detailed description.

        Args:
            view: The view object being configured
            include_description_key: Whether to include detailed description for teacher types
        """
        metadata = {
            "presentation": {"title_public": generate_title_by_dimensions(view)},
            "description_short": generate_subtitle_by_dimensions(view),
        }
        if include_description_key:
            metadata["description_key"] = TEACHER_TYPE_DESCRIPTION_KEY
        return metadata

    def get_view_config(view):
        """Generate chart configuration for grouped views."""
        return GROUPED_VIEW_CONFIG | {
            "title": generate_title_by_dimensions(view),
            "subtitle": generate_subtitle_by_dimensions(view),
        }

    collection.group_views(
        groups=[
            {
                # Group 1: Compare qualified vs trained teachers side-by-side
                "dimension": "teacher_type",
                "choice_new_slug": "teacher_type_side_by_side",
                "choices": ["qualified", "trained"],
                "view_config": get_view_config,
                "view_metadata": lambda view: get_view_metadata(view, include_description_key=True),
            },
            {
                # Group 2: Compare education levels side-by-side for one teacher type
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "choices": list(EDUCATION_LEVELS.keys()),  # All education levels
                "view_config": get_view_config,
                "view_metadata": get_view_metadata,
            },
        ],
    )


def generate_title_by_dimensions(view):
    """Generate chart title for grouped views only.

    Handles two grouped view types:
    1. Level comparison: "Qualified teachers by education level"
    2. Teacher type comparison: "Qualified and trained teachers in primary education"
    """
    if view.matches(level="level_side_by_side"):
        # Comparing one teacher type across education levels
        teacher_type = view.dimensions.get("teacher_type", "qualified")
        teacher_cfg = TEACHER_TYPES.get(teacher_type, {})
        teacher_title = teacher_cfg.get("title", "Teachers")
        return f"{teacher_title} by education level"

    elif view.matches(teacher_type="teacher_type_side_by_side"):
        # Comparing qualified vs trained for specific education level
        level = view.dimensions.get("level", "primary")
        level_cfg = EDUCATION_LEVELS.get(level, {})
        level_term = level_cfg.get("title_term", level)
        return f"Qualified and trained teachers in {level_term}"


def generate_subtitle_by_dimensions(view):
    """Generate descriptive subtitle for grouped views only.

    Creates subtitles that:
    1. Use proper DOD links for education levels
    2. Explain what qualified vs trained means for grouped views
    3. Include "shown as a percentage of all teachers teaching at this level" for clarity
    4. Adjust text based on whether it's qualified or trained teachers
    """

    if view.matches(level="level_side_by_side"):
        # Education level comparison: get the specific teacher type
        teacher_type = view.dimensions["teacher_type"]
        teacher_cfg = TEACHER_TYPES.get(teacher_type, {})
        teacher_full_description = teacher_cfg.get("full_description", "qualifications")

        # Build comma-separated list with "and" at the end
        education_levels = ", ".join([config["subtitle_link"] for config in EDUCATION_LEVELS.values()])
        education_levels = education_levels.rsplit(", ", 1)  # Split on last comma to add "and"
        education_levels = " and ".join(education_levels)  # Join with "and"

        return f"Share of teachers {teacher_full_description} across {education_levels} education, shown as a percentage of all teachers teaching at each level."

    elif view.matches(teacher_type="teacher_type_side_by_side"):
        # Teacher type comparison: get specific education level link
        level = view.dimensions.get("level", "primary")
        education_level = EDUCATION_LEVELS.get(level, EDUCATION_LEVELS["primary"])["subtitle_link"]

        # Get full descriptions for both teacher types
        qualified_desc = TEACHER_TYPES["qualified"]["full_description"]
        trained_desc = TEACHER_TYPES["trained"]["full_description"]

        return f"Share of teachers {qualified_desc} (qualified) and teachers {trained_desc} (trained) in {education_level} education, shown as a percentage of all teachers teaching at this level."


def edit_indicator_displays(view):
    """Clean up indicator display names and colors for better chart labels in grouped views.

    Sets concise, readable names and appropriate colors for indicators in grouped views:
    - Level comparisons: "Primary", "Secondary", etc. with level-specific colors
    - Teacher type comparisons: "Qualified teachers", "Trained teachers" with type-specific colors
    """

    # Color mappings for education levels
    level_colors = {
        "pre_primary": COLOR_PREPRIMARY,
        "primary": COLOR_PRIMARY,
        "lower_secondary": COLOR_LOWER_SECONDARY,
        "upper_secondary": COLOR_UPPER_SECONDARY,
    }

    # Color mappings for teacher types
    teacher_type_colors = {
        "qualified": COLOR_QUALIFIED,
        "trained": COLOR_TRAINED,
    }

    for ind in view.indicators.y:
        if view.matches(level="level_side_by_side"):
            # Education level comparison: use short education level names and colors
            for level, config in EDUCATION_LEVELS.items():
                if level in ind.catalogPath:
                    ind.display = {"name": config["display_name"], "color": level_colors.get(level, COLOR_PRIMARY)}
                    break
        elif view.matches(teacher_type="teacher_type_side_by_side"):
            # Teacher type comparison: use teacher type titles and colors
            for teacher_type, config in TEACHER_TYPES.items():
                if config["catalog_key"] in ind.catalogPath:
                    ind.display = {
                        "name": config["title"],
                        "color": teacher_type_colors.get(teacher_type, COLOR_QUALIFIED),
                    }
                    break

    # Sort indicators for level_side_by_side: pre-primary → primary → lower secondary → upper secondary
    if view.matches(level="level_side_by_side"):

        def get_level_index(ind):
            if "pre_primary" in ind.catalogPath:
                return 0
            elif "primary" in ind.catalogPath and "pre" not in ind.catalogPath:
                return 1
            elif "lower_secondary" in ind.catalogPath:
                return 2
            elif "upper_secondary" in ind.catalogPath:
                return 3
            return 4

        view.indicators.y = sorted(view.indicators.y, key=get_level_index)

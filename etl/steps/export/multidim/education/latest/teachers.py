"""Create multidimensional collection for qualified and trained teachers data."""

import re

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# --------------------- #
#   Constants & Config  #
# --------------------- #

ID_COLUMNS = ["country", "year"]

QUALIFIED_TEACHER_PATTERN = r"percentage_of_qualified_teachers_in_(pre_primary|primary|lower_secondary|upper_secondary)_education__both_sexes"
TRAINED_TEACHER_PATTERN = r"^(se_pre_tcaq_zs|se_prm_tcaq_zs|se_sec_tcaq_lo_zs|se_sec_tcaq_up_zs)$"

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
    "pre_primary": {
        "keywords": ["pre", "preprimary"],
        "wdi_code": "se_pre_tcaq_zs",
        "display_name": "Pre-primary education",
        "title_term": "pre-primary education",
        "age_range": "pre-primary education (typically ages 3-5)",
    },
    "primary": {
        "keywords": ["prm", "primary"],
        "wdi_code": "se_prm_tcaq_zs",
        "display_name": "Primary education",
        "title_term": "primary education",
        "age_range": "primary education (typically ages 6-11)",
    },
    "lower_secondary": {
        "keywords": ["sec", "lower", "lo"],
        "wdi_code": "se_sec_tcaq_lo_zs",
        "display_name": "Lower secondary education",
        "title_term": "lower secondary education",
        "age_range": "lower secondary education (typically ages 12-14)",
    },
    "upper_secondary": {
        "keywords": ["sec", "upper", "up"],
        "wdi_code": "se_sec_tcaq_up_zs",
        "display_name": "Upper secondary education",
        "title_term": "upper secondary education",
        "age_range": "upper secondary education (typically ages 15-17)",
    },
}

TEACHER_TYPES = {
    "qualified": {
        "title": "Qualified teachers",
        "unit": "percent",
        "y_axis": {"min": 0, "max": 100},
        "description": "shown as a percentage of all teachers in the relevant education level",
        "source_pattern": QUALIFIED_TEACHER_PATTERN,
    },
    "trained": {
        "title": "Trained teachers",
        "unit": "percent",
        "y_axis": {"min": 0, "max": 100},
        "description": "shown as a percentage of all teachers in the relevant education level",
        "source_pattern": TRAINED_TEACHER_PATTERN,
    },
}


def run() -> None:
    """Main function to process teachers data and create collection."""
    config = paths.load_collection_config()
    tbs_adjusted = []

    for dataset_name in ["education_sdgs", "wdi"]:
        ds = paths.load_dataset(dataset_name)
        tb = ds.read(dataset_name, load_data=False)

        teacher_cols = get_teacher_columns(tb, dataset_name)
        print(teacher_cols)
        if teacher_cols:
            tb = tb.loc[:, ID_COLUMNS + teacher_cols].copy()
            tb = adjust_dimensions(tb, dataset_name)
            tbs_adjusted.append(tb)

    collection = paths.create_collection(
        config=config,
        tb=tbs_adjusted,
        common_view_config=MULTIDIM_CONFIG,
    )

    create_grouped_views(collection)

    collection.set_global_config(
        config={
            "title": lambda view: generate_title_by_dimensions(view),
            "subtitle": lambda view: generate_subtitle_by_dimensions(view),
        }
    )

    for view in collection.views:
        edit_indicator_displays(view)

    collection.save()


def get_teacher_columns(tb, dataset_name):
    """Filter teacher columns based on dataset source."""
    if dataset_name == "education_sdgs":
        return [col for col in tb.columns if re.search(QUALIFIED_TEACHER_PATTERN, col, re.IGNORECASE)]
    elif dataset_name == "wdi":
        return [col for col in tb.columns if re.search(TRAINED_TEACHER_PATTERN, col, re.IGNORECASE)]
    return []


def adjust_dimensions(tb, dataset_name):
    """Add dimensions to teacher data columns."""

    def extract_level_from_qualified(col):
        col_lower = col.lower()
        if "pre" in col_lower or "preprimary" in col_lower:
            return "pre_primary"
        elif "primary" in col_lower and "lower" not in col_lower and "upper" not in col_lower:
            return "primary"
        elif "lower" in col_lower and "secondary" in col_lower:
            return "lower_secondary"
        elif "upper" in col_lower and "secondary" in col_lower:
            return "upper_secondary"
        elif "secondary" in col_lower:
            return "lower_secondary"  # Default secondary to lower secondary
        return "primary"  # Default fallback

    def extract_level_from_wdi(col):
        for level, config in EDUCATION_LEVELS.items():
            if col.lower() == config["wdi_code"].lower():
                return level
        return "primary"  # Default fallback

    def extract_teacher_type(dataset_name):
        if dataset_name == "education_sdgs":
            return "qualified"
        elif dataset_name == "wdi":
            return "trained"
        return "qualified"  # Default fallback

    for col in tb.columns:
        if col in ID_COLUMNS:
            continue

        tb[col].metadata.original_short_name = "teachers_qualified_trained"

        if dataset_name == "education_sdgs":
            level = extract_level_from_qualified(col)
        else:
            level = extract_level_from_wdi(col)

        teacher_type = extract_teacher_type(dataset_name)

        tb[col].metadata.dimensions = {
            "level": level,
            "teacher_type": teacher_type,
        }

    tb.metadata.dimensions.extend(
        [
            {"name": "Education level", "slug": "level"},
            {"name": "Teacher type", "slug": "teacher_type"},
        ]
    )

    return tb


def create_grouped_views(collection):
    """Add grouped views for education level and teacher type comparisons."""

    view_metadata = {
        "presentation": {"title_public": "{title}"},
        "description_short": "{subtitle}",
    }

    def get_view_config(_):
        return GROUPED_VIEW_CONFIG | {
            "title": "{title}",
            "subtitle": "{subtitle}",
        }

    collection.group_views(
        groups=[
            {
                "dimension": "teacher_type",
                "choice_new_slug": "teacher_type_side_by_side",
                "choices": ["qualified", "trained"],
                "view_config": get_view_config,
                "view_metadata": view_metadata,
            },
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "choices": ["pre_primary", "primary", "lower_secondary", "upper_secondary"],
                "view_config": get_view_config,
                "view_metadata": view_metadata,
            },
        ],
        params={
            "title": lambda view: generate_title_by_dimensions(view),
            "subtitle": lambda view: generate_subtitle_by_dimensions(view),
        },
    )


def generate_title_by_dimensions(view):
    """Generate chart title based on view dimensions."""
    level = view.dimensions.get("level", "primary")
    teacher_type = view.dimensions.get("teacher_type", "qualified")

    level_cfg = EDUCATION_LEVELS.get(level, {})
    teacher_cfg = TEACHER_TYPES.get(teacher_type, {})
    level_term = level_cfg.get("title_term", level)
    teacher_title = teacher_cfg.get("title", "Teachers")

    if view.matches(level="level_side_by_side"):
        if view.matches(teacher_type="teacher_type_side_by_side"):
            return "Qualified and trained teachers, by education level"
        else:
            return f"{teacher_title} by education level"
    elif view.matches(teacher_type="teacher_type_side_by_side"):
        return f"Qualified and trained teachers in {level_term}"
    else:
        return f"{teacher_title} in {level_term}"


def generate_subtitle_by_dimensions(view):
    """Generate chart subtitle based on dimensions."""
    level = view.dimensions.get("level", "primary")
    teacher_type = view.dimensions.get("teacher_type", "qualified")

    level_cfg = EDUCATION_LEVELS.get(level, {})
    teacher_cfg = TEACHER_TYPES.get(teacher_type, {})
    age_range = level_cfg.get("age_range", "")
    description = teacher_cfg.get("description", "")

    if view.matches(level="level_side_by_side"):
        return f"Share of teachers who are qualified or trained across different education levels, {description}."
    elif view.matches(teacher_type="teacher_type_side_by_side"):
        return f"Share of qualified and trained teachers in {age_range}, {description}."
    else:
        return f"Share of {teacher_type} teachers in {age_range}, {description}."


def edit_indicator_displays(view):
    """Clean up indicator display names for grouped views."""

    level_display = {
        "pre_primary": "Pre-primary",
        "primary": "Primary",
        "lower_secondary": "Lower secondary",
        "upper_secondary": "Upper secondary",
    }

    teacher_type_display = {
        "qualified": "Qualified teachers",
        "trained": "Trained teachers",
    }

    for ind in view.indicators.y:
        if view.matches(level="level_side_by_side"):
            for k, v in level_display.items():
                if k in ind.catalogPath:
                    ind.display = {"name": v}
                    break
        elif view.matches(teacher_type="teacher_type_side_by_side"):
            for k, v in teacher_type_display.items():
                if k in ind.catalogPath:
                    ind.display = {"name": v}
                    break

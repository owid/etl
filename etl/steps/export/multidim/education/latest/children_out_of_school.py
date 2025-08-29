"""Create multidimensional collection for children out of school data."""

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
COLOR_TERTIARY = "#B16214"

COLOR_BOYS = "#00847E"
COLOR_GIRLS = "#E56E5A"

ID_COLUMNS = ["country", "year"]

OUT_OF_SCHOOL_PATTERN = r"out_of_school"

EXCLUSION_PATTERNS = [
    "urban",
    "rural",
    "quintile",
    "adjusted",
    "modelled_data",
    "very_affluent",
    "very_poor",
    "immigrant",
    "native",
    "disabled",
    "not_disabled",
    "primary_and_lower_secondary",
    "primary_and_secondary",
    "lower_and_upper_secondary",
    "primary__lower_secondary_and_upper_secondary",
    "household_survey_data",
    "out_of_school_adolescents_and_youth_of_secondary",
]
EXCLUSION_REGEX = re.compile("|".join(EXCLUSION_PATTERNS), re.IGNORECASE)

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
    "yAxis": {"min": 0, "facetDomain": "independent"},
    "selectedFacetStrategy": "entity",
    "addCountryMode": "add-country",
}

# --------------------- #
#      Dimensions       #
# --------------------- #


EDUCATION_LEVELS = {
    "primary": {
        "keywords": ["primary_school_age"],
        "display_name": "Primary education",
        "title_term": "primary",
        "age_range": "of primary school age (typically 6–11 years)",
        "dod_term": "primary-education",
    },
    "lower_secondary": {
        "keywords": ["lower_secondary_school_age"],
        "display_name": "Lower secondary education",
        "title_term": "lower secondary",
        "age_range": "of lower secondary school age (typically 12–14 years)",
        "dod_term": "lower-secondary-education",
    },
    "upper_secondary": {
        "keywords": ["upper_secondary_school_age"],
        "display_name": "Upper secondary education",
        "title_term": "upper secondary",
        "age_range": "of upper secondary school age (typically 15–17 years)",
        "dod_term": "upper-secondary-education",
    },
    "pre_primary": {
        "keywords": ["one_year_before_the_official_primary_entry_age"],
        "display_name": "Pre-primary education",
        "title_term": "pre-primary",
        "age_range": "one year before official primary entry age (typically age 5)",
        "dod_term": "pre-primary-education",
    },
}

LEVEL_KEYWORDS = {kw: level for level, cfg in EDUCATION_LEVELS.items() for kw in cfg["keywords"]}

SEX_KEYWORDS = {
    "both_sexes": "both",
    "_male": "male",
    "_female": "female",
}

GENDERS = {
    "both": "children",
    "male": "boys",
    "female": "girls",
    "sex_side_by_side": "boys and girls",
}


def run() -> None:
    """Main function to process children out of school data and create collection."""
    config = paths.load_collection_config()
    tbs_adjusted = []

    for dataset_name in ["education_opri", "education_sdgs"]:
        ds = paths.load_dataset(dataset_name)
        tb = ds.read(dataset_name, load_data=False)

        out_of_school_cols = get_out_of_school_columns(tb)
        tb = tb.loc[:, ID_COLUMNS + out_of_school_cols].copy()
        tb = adjust_dimensions(tb)
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
        # Update title and subtitle based on view dimensions
        sex = view.dimensions["sex"]
        level = view.dimensions["level"]
        # Set view metadata for all views
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
                "description_short": view.config["subtitle"],
                "presentation": {
                    "title_public": view.config["title"],
                },
            }
        edit_indicator_displays(view)

    collection.save()


def get_out_of_school_columns(tb):
    """Filter out-of-school columns excluding unwanted categories."""
    return [
        col
        for col in tb.columns
        if re.search(OUT_OF_SCHOOL_PATTERN, col, re.IGNORECASE) and not EXCLUSION_REGEX.search(col)
    ]


def adjust_dimensions(tb):
    """Add dimensions to out-of-school data columns."""

    def extract_level(col):
        for kw, level in LEVEL_KEYWORDS.items():
            if kw in col:
                return level
        return None

    def extract_sex(col):
        for kw, sex in SEX_KEYWORDS.items():
            if kw in col:
                return sex
        return "both"

    def extract_metric_type(col):
        if any(w in col.lower() for w in ["rate", "percent", "pct", "share", "%"]):
            return "rate"
        elif any(w in col.lower() for w in ["number", "count", "total"]):
            return "number"
        return "rate"

    for col in tb.columns:
        if col in ID_COLUMNS:
            continue
        tb[col].metadata.original_short_name = "children_out_of_school"
        tb[col].metadata.dimensions = {
            "level": extract_level(col),
            "sex": extract_sex(col),
            "metric_type": extract_metric_type(col),
        }

    tb.metadata.dimensions.extend(
        [
            {"name": "Education level", "slug": "level"},
            {"name": "Gender", "slug": "sex"},
            {"name": "Metric type", "slug": "metric_type"},
        ]
    )

    return tb


def create_grouped_views(collection):
    """Add grouped views for gender, education level, and metric type comparisons."""

    view_metadata = {
        "presentation": {"title_public": "{title}"},
        "description_short": "{subtitle}",
    }

    def get_view_config(view):
        return GROUPED_VIEW_CONFIG | {
            "title": "{title}",
            "subtitle": "{subtitle}",
        }

    collection.group_views(
        groups=[
            {
                "dimension": "sex",
                "choice_new_slug": "sex_side_by_side",
                "choices": ["female", "male"],
                "view_config": get_view_config,
                "view_metadata": view_metadata,
            },
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "choices": ["primary", "lower_secondary", "upper_secondary"],
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
    sex = view.dimensions.get("sex", "both")
    level = view.dimensions.get("level", "primary")
    metric = view.dimensions.get("metric_type", "rate")
    gender_term = GENDERS.get(sex, "children")
    level_cfg = EDUCATION_LEVELS.get(level, {})
    title_term = level_cfg.get("title_term", "")

    # ---- RATE ----
    if metric == "rate":
        if view.matches(level="level_side_by_side"):
            return f"{gender_term.title()} of official age for each level of education who are not in school"
        if view.matches(sex="sex_side_by_side"):
            return f"Share of girls and boys who are not in {title_term} school"
        else:
            return f"Share of {gender_term} who are not in {title_term} school"

    # ---- NUMBER ----
    else:
        if view.matches(level="level_side_by_side"):
            return f"Number of {gender_term} of official age for each level of education who are not in school"
        else:
            return f"Number of {gender_term} who are not in {title_term} school"


def _dod_link(level_cfg):
    """Return markdown link [title_term](#dod:slug) if dod_term exists."""
    title = level_cfg.get("title_term", "")
    dod_term = level_cfg.get("dod_term")
    if dod_term:
        # Use first occurrence of the key term (primary / secondary etc.)
        short_label = title.split(" school age")[0] if "school age" in title else title
        return f"[{short_label}](#dod:{dod_term})"
    return title


def generate_subtitle_by_dimensions(view):
    """Generate chart subtitle with DOD links included."""
    level = view.dimensions.get("level", "primary")
    sex = view.dimensions.get("sex", "both")
    metric = view.dimensions.get("metric_type", "rate")

    level_cfg = EDUCATION_LEVELS.get(level, {})
    gender_term = GENDERS.get(sex, "children")
    title_term = level_cfg.get("title_term", "")
    age_range = level_cfg.get("age_range", "")
    dod_link = _dod_link(level_cfg)

    # Determine scope
    term_lower = title_term.lower()
    if "upper secondary" in term_lower:
        scope = f"{dod_link}"
    elif "lower secondary" in term_lower:
        scope = f"{dod_link}"
    elif "primary" in term_lower and "before" not in term_lower:
        scope = f"{dod_link}"
    elif "pre-primary" in term_lower:
        scope = f"{dod_link}"
    else:
        scope = dod_link or title_term

    # ---- RATE ----
    if metric == "rate":
        if view.matches(level="level_side_by_side"):
            return (
                f"{gender_term.title()} of official age for each level of education not enrolled at that level of education, "
                f"expressed as a percentage of the total population of {gender_term} in that age group."
            )
        else:
            return (
                f"{gender_term.title()} {age_range} not enrolled in {scope} education, "
                f"expressed as a percentage of the total population of {gender_term} in that age group."
            )

    # ---- NUMBER ----
    else:
        if view.matches(level="level_side_by_side"):
            return f"{gender_term.title()} of official age for each level of education not enrolled in that level of education or higher."
        else:
            return f"{gender_term.title()} {age_range} not enrolled in {scope} education or higher."


def edit_indicator_displays(view):
    """Clean up indicator display names and colors for grouped views."""

    level_display = {
        "pre_primary": {"name": "Pre-primary", "color": COLOR_PREPRIMARY},
        "primary": {"name": "Primary", "color": COLOR_PRIMARY},
        "lower_secondary": {"name": "Lower secondary", "color": COLOR_LOWER_SECONDARY},
        "upper_secondary": {"name": "Upper secondary", "color": COLOR_UPPER_SECONDARY},
    }

    sex_display = {
        "_male": {"name": "Boys", "color": COLOR_BOYS},
        "_female": {"name": "Girls", "color": COLOR_GIRLS},
    }

    for ind in view.indicators.y:
        if view.matches(level="level_side_by_side"):
            for k, config in level_display.items():
                if k in ind.catalogPath:
                    ind.display = {"name": config["name"], "color": config["color"]}
                    break
        elif view.matches(sex="sex_side_by_side"):
            for k, config in sex_display.items():
                if k in ind.catalogPath:
                    ind.display = {"name": config["name"], "color": config["color"]}
                    break

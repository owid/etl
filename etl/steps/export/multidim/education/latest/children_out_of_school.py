"""Create multidimensional collection for children out of school data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "hasMapTab": True,
    "tab": "map",
    "addCountryMode": "add-country",
}

# Common grouped view configuration
GROUPED_VIEW_CONFIG = MULTIDIM_CONFIG | {
    "hasMapTab": False,
    "tab": "chart",
    "yAxis": {"min": 0, "facetDomain": "independent"},
    "selectedFacetStrategy": "entity",
    "addCountryMode": "change-country",
}

# Education level configurations
EDUCATION_LEVELS = {
    "primary": {
        "keywords": ["primary_school_age"],
        "display_name": "Primary education",
        "title_term": "primary school age",
        "age_range": "primary school age (typically 6-11 years)",
    },
    "lower_secondary": {
        "keywords": ["lower_secondary_school_age"],
        "display_name": "Lower secondary education",
        "title_term": "lower secondary school age",
        "age_range": "lower secondary school age (typically 12-14 years)",
    },
    "upper_secondary": {
        "keywords": ["upper_secondary_school_age"],
        "display_name": "Upper secondary education",
        "title_term": "upper secondary school age",
        "age_range": "upper secondary school age (typically 15-17 years)",
    },
    "pre_primary": {
        "keywords": ["one_year_before_the_official_primary_entry_age"],
        "display_name": "Pre-primary education",
        "title_term": "pre-primary age",
        "age_range": "one year before official primary entry age (typically age 5)",
    },
}

# Gender configurations
GENDERS = {
    "both": "children",
    "male": "boys",
    "female": "girls",
    "sex_side_by_side": "boys and girls",
}

# Metric type configurations
METRIC_TYPES = {
    "rate": {
        "title": "Share of children out of school",
        "unit": "percent",
        "y_axis": {"min": 0, "max": 100},
        "description": "shown as a percentage of children in the relevant age group",
    },
    "number": {
        "title": "Number of children out of school",
        "unit": "children",
        "y_axis": {"min": 0},
        "description": "shown as the total number of children not enrolled in school",
    },
}

# Dimension mapping configurations
LEVEL_KEYWORDS = {}
for level_key, config in EDUCATION_LEVELS.items():
    for keyword in config["keywords"]:
        LEVEL_KEYWORDS[keyword] = level_key

SEX_KEYWORDS = {"both_sexes": "both", "_male": "male", "_female": "female"}

# Exclusion patterns for column filtering - exclude disability, combined age groups, and data source variations
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
    "primary_and_lower_secondary",  # Combined age groups
    "primary_and_secondary",  # Combined age groups
    "lower_and_upper_secondary",  # Combined age groups
    "primary__lower_secondary_and_upper_secondary",  # Combined age groups
    "household_survey_data",  # Keep only modelled data for consistency
    "out_of_school_adolescents_and_youth_of_secondary",
]


def run() -> None:
    """Main function to process children out of school data and create collection."""
    # Load inputs
    config = paths.load_collection_config()

    # Try both datasets and combine if both exist
    tbs_adjusted = []

    for dataset_name in ["education_opri", "education_sdgs"]:
        ds = paths.load_dataset(dataset_name)
        tb = ds.read(dataset_name, load_data=False)

        out_of_school_cols = get_out_of_school_columns(tb)
        # Select only relevant columns
        tb = tb.loc[:, ["country", "year"] + out_of_school_cols].copy()

        tb = adjust_dimensions(tb)
        tbs_adjusted.append(tb)

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=[tbs_adjusted[0], tbs_adjusted[1]],  # Use both datasets
        common_view_config=MULTIDIM_CONFIG,
    )

    # Add grouped views
    create_grouped_views(c)

    # Set global configuration
    c.set_global_config(
        config={
            "title": lambda view: generate_title_by_dimensions(view),
            "subtitle": lambda view: generate_subtitle_by_dimensions(view),
        }
    )

    # Edit display names
    for view in c.views:
        edit_indicator_displays(view)

    # Save collection
    c.save()


def get_out_of_school_columns(tb):
    """Filter out of school columns excluding unwanted categories."""
    import re

    # Pattern to match out of school columns
    out_of_school_pattern = r"out_of_school"

    # Create exclusion pattern
    exclusion_pattern = "|".join(EXCLUSION_PATTERNS)

    # Get all matching columns
    all_cols = [
        col
        for col in tb.columns
        if re.search(out_of_school_pattern, col, re.IGNORECASE) and not re.search(exclusion_pattern, col, re.IGNORECASE)
    ]

    return all_cols


def adjust_dimensions(tb):
    """Add dimensions to out of school table columns."""

    def _extract_education_level(column_name):
        """Extract education level from column name."""
        if "primary_school_age" in column_name and "secondary" not in column_name:
            return "primary"
        elif "lower_secondary_school_age" in column_name:
            return "lower_secondary"
        elif "upper_secondary_school_age" in column_name:
            return "upper_secondary"
        elif "one_year_before_the_official_primary_entry_age" in column_name:
            return "pre_primary"
        return None

    def _extract_gender(column_name):
        """Extract gender dimension from column name."""
        for keyword, value in SEX_KEYWORDS.items():
            if keyword in column_name:
                return value
        return "both"  # Default

    def _extract_metric_type(column_name):
        """Extract metric type from column name."""
        if any(word in column_name.lower() for word in ["rate", "percent", "pct", "share"]):
            return "rate"
        elif any(word in column_name.lower() for word in ["number", "count", "total"]):
            return "number"
        else:
            # Default based on common patterns
            return "rate" if "%" in column_name else "number"

    # Process each column
    for col in tb.columns:
        if col in ["country", "year"]:
            continue

        # Extract dimensions
        level = _extract_education_level(col)
        sex = _extract_gender(col)
        metric_type = _extract_metric_type(col)

        # Set indicator name and dimensions
        tb[col].metadata.original_short_name = "children_out_of_school"
        tb[col].metadata.dimensions = {
            "level": level,
            "sex": sex,
            "metric_type": metric_type,
        }

    # Add dimension definitions to table metadata
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
        """Get view config with conditional chart types based on metric type."""
        base_config = GROUPED_VIEW_CONFIG | {
            "title": "{title}",
            "subtitle": "{subtitle}",
        }

        return base_config

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
    """Generate title based on dimensions."""
    sex = view.dimensions.get("sex", "both")
    level = view.dimensions.get("level", "primary")
    metric_type = view.dimensions.get("metric_type", "rate")

    gender_term = GENDERS.get(sex, "children")
    level_config = EDUCATION_LEVELS.get(level, {})
    age_term = level_config.get("title_term", level)

    # Handle different view combinations
    if view.matches(level="level_side_by_side"):
        if view.matches(sex="sex_side_by_side"):
            if metric_type == "rate":
                return "Share of children out of school, by education level and gender"
            else:
                return "Number of children out of school, by education level and gender"
        else:
            if metric_type == "rate":
                return f"Share of {gender_term} out of school, by education level"
            else:
                return f"Number of {gender_term} out of school, by education level"
    elif view.matches(sex="sex_side_by_side"):
        if metric_type == "rate":
            return f"Share of children of {age_term} who are not in school, by gender"
        else:
            return f"Number of children of {age_term} who are not in school, by gender"
    else:
        if metric_type == "rate":
            return f"Share of {gender_term} of {age_term} who are not in school"
        else:
            return f"Number of {gender_term} of {age_term} who are not in school"


def generate_subtitle_by_dimensions(view):
    """Generate subtitle based on dimensions."""
    level = view.dimensions.get("level", "primary")
    metric_type = view.dimensions.get("metric_type", "rate")
    sex = view.dimensions.get("sex", "both")

    level_config = EDUCATION_LEVELS.get(level, {})
    gender_term = GENDERS.get(sex, "children")

    age_range = level_config.get("age_range", "school age")

    # Generate gender-specific description based on metric type
    if metric_type == "rate":
        if sex == "both" or view.matches(sex="sex_side_by_side"):
            description = "shown as a percentage of children in the relevant age group"
        else:
            description = f"shown as a percentage of {gender_term} in the relevant age group"
    else:  # number
        if sex == "both" or view.matches(sex="sex_side_by_side"):
            description = "shown as the total number of children not enrolled in school"
        else:
            description = f"shown as the total number of {gender_term} not enrolled in school"

    if view.matches(level="level_side_by_side"):
        return f"{gender_term.title()} not enrolled in school across different education levels, {description}."
    elif view.matches(sex="sex_side_by_side"):
        return f"Children of {age_range} not enrolled in school, {description}."
    else:
        return f"{gender_term.title()} of {age_range} not enrolled in school, {description}."


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.indicators.y is None:
        return

    display_names = {
        "level": {
            "pre_primary": "Pre-primary",
            "primary": "Primary",
            "lower_secondary": "Lower secondary",
            "upper_secondary": "Upper secondary",
        },
        "sex": {
            "_male": "Boys",
            "_female": "Girls",
            "both": "Both genders",
        },
    }

    for indicator in view.indicators.y:
        if view.matches(level="level_side_by_side"):
            for level_key, display_name in display_names["level"].items():
                if level_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break
        elif view.matches(sex="sex_side_by_side"):
            for sex_key, display_name in display_names["sex"].items():
                if sex_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break

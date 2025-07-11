"""Create multidimensional collection for children out of school data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "yAxis": {"min": 0},
    "hasMapTab": True,
    "tab": "map",
    "addCountryMode": "change-country",
}

# Common grouped view configuration
GROUPED_VIEW_CONFIG = MULTIDIM_CONFIG | {
    "hasMapTab": False,
    "tab": "chart",
    "selectedFacetStrategy": "entity",
}

# Education level configurations
EDUCATION_LEVELS = {
    "primary": {
        "keywords": ["primary"],
        "display_name": "Primary education",
        "title_term": "primary school age",
        "age_range": "primary school age (typically 6-11 years)",
    },
    "lower_secondary": {
        "keywords": ["lower_secondary", "lowersec"],
        "display_name": "Lower secondary education",
        "title_term": "lower secondary school age",
        "age_range": "lower secondary school age (typically 12-14 years)",
    },
    "upper_secondary": {
        "keywords": ["upper_secondary", "uppersec"],
        "display_name": "Upper secondary education",
        "title_term": "upper secondary school age",
        "age_range": "upper secondary school age (typically 15-17 years)",
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
        "description": "as a percentage of children in the relevant age group",
    },
    "number": {
        "title": "Number of children out of school",
        "unit": "children",
        "y_axis": {"min": 0},
        "description": "total number of children not enrolled in school",
    },
}

# Dimension mapping configurations
LEVEL_KEYWORDS = {}
for level_key, config in EDUCATION_LEVELS.items():
    for keyword in config["keywords"]:
        LEVEL_KEYWORDS[keyword] = level_key

SEX_KEYWORDS = {"both_sexes": "both", "male": "male", "female": "female"}

# Exclusion patterns for column filtering
EXCLUSION_PATTERNS = [
    "urban",
    "rural",
    "poorest_quintile",
    "richest_quintile",
    "adjusted",
    "very_affluent",
    "very_poor",
    "immigrant",
    "native",
]


def run() -> None:
    """Main function to process children out of school data and create collection."""
    # Load inputs
    config = paths.load_collection_config()

    # Try both datasets and combine if both exist
    datasets = []

    ds_opri = paths.load_dataset("education_opri")
    datasets.append(("opri", ds_opri))

    ds_sdg = paths.load_dataset("education_sdg")
    datasets.append(("sdg", ds_sdg))
    tbs_adjusted = []
    for _, ds in datasets:
        # Get first table from dataset
        table_name = list(ds.table_names)[0]
        tb = ds.read(table_name, load_data=False)

        # Filter out of school columns
        out_of_school_cols = get_out_of_school_columns(tb)

        # Select only relevant columns
        tb = tb.loc[:, ["country", "year"] + out_of_school_cols].copy()
        print(tb.columns)

        tb = adjust_dimensions(tb)
        tbs_adjusted.append(tb)

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=tb,
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

    return [
        col
        for col in tb.columns
        if re.search(out_of_school_pattern, col, re.IGNORECASE) and not re.search(exclusion_pattern, col, re.IGNORECASE)
    ]


def adjust_dimensions(tb):
    """Add dimensions to out of school table columns."""

    def _extract_dimension(column_name, keyword_map):
        """Extract dimension value from column name using keyword mapping."""
        for keyword, value in keyword_map.items():
            if keyword in column_name:
                return value
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
        level = _extract_dimension(col, LEVEL_KEYWORDS)
        sex = _extract_gender(col)
        metric_type = _extract_metric_type(col)

        # Set indicator name and dimensions
        tb[col].metadata.original_short_name = "children_out_of_school"
        tb[col].metadata.dimensions = {
            "level": level or "primary",
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
    view_config = GROUPED_VIEW_CONFIG | {
        "title": "{title}",
        "subtitle": "{subtitle}",
    }

    collection.group_views(
        groups=[
            {
                "dimension": "sex",
                "choice_new_slug": "sex_side_by_side",
                "choices": ["female", "male"],
                "view_config": view_config,
                "view_metadata": view_metadata,
            },
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "choices": ["primary", "lower_secondary", "upper_secondary"],
                "view_config": view_config,
                "view_metadata": view_metadata,
            },
            {
                "dimension": "metric_type",
                "choice_new_slug": "metric_type_side_by_side",
                "choices": ["rate", "number"],
                "view_config": view_config,
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
    metric_config = METRIC_TYPES.get(metric_type, {})

    base_title = metric_config.get("title", "Children out of school")

    # Handle different view combinations
    if view.matches(metric_type="metric_type_side_by_side"):
        return f"Children out of school of {level_config.get('title_term', level)}, by metric type"
    elif view.matches(level="level_side_by_side"):
        if view.matches(sex="sex_side_by_side"):
            return f"{base_title}, by education level and gender"
        else:
            return f"{base_title} ({gender_term}), by education level"
    elif view.matches(sex="sex_side_by_side"):
        return f"{base_title} of {level_config.get('title_term', level)}, by gender"
    else:
        return f"{base_title} ({gender_term} of {level_config.get('title_term', level)})"


def generate_subtitle_by_dimensions(view):
    """Generate subtitle based on dimensions."""
    level = view.dimensions.get("level", "primary")
    metric_type = view.dimensions.get("metric_type", "rate")

    level_config = EDUCATION_LEVELS.get(level, {})
    metric_config = METRIC_TYPES.get(metric_type, {})

    age_range = level_config.get("age_range", "school age")
    description = metric_config.get("description", "")

    # Handle different view types
    if view.matches(level="level_side_by_side") and view.matches(sex="sex_side_by_side"):
        return f"Children not enrolled in school across different education levels and by gender. Data shows {description}."
    elif view.matches(level="level_side_by_side"):
        return f"Children not enrolled in school across different education levels. Data shows {description}."
    elif view.matches(sex="sex_side_by_side"):
        return f"Children of {age_range} not enrolled in school, by gender. Data shows {description}."
    elif view.matches(metric_type="metric_type_side_by_side"):
        return f"Children of {age_range} not enrolled in school, shown both as a percentage and absolute numbers."
    else:
        return f"Children of {age_range} not enrolled in school. Data shows {description}."


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.indicators.y is None:
        return

    display_names = {
        "level": {
            "primary": "Primary education",
            "lower_secondary": "Lower secondary education",
            "upper_secondary": "Upper secondary education",
        },
        "sex": {
            "male": "Boys",
            "female": "Girls",
            "both": "Both genders",
        },
        "metric_type": {
            "rate": "Share (%)",
            "number": "Number",
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
        elif view.matches(metric_type="metric_type_side_by_side"):
            for metric_key, display_name in display_names["metric_type"].items():
                if metric_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break

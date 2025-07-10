"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "yAxis": {"min": 300, "max": 600},
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

# Column filtering patterns
PISA_PATTERNS = {
    "mathematics": ["pisa_math", "boys_girls", "average"],
    "science": ["pisa_science", "all", "average"],
    "reading": ["pisa_reading", "boys_girls", "average"],
}

# Subject configurations
SUBJECTS = {
    "mathematics": {
        "keywords": "math",
        "display_name": "Mathematics",
        "title_term": "mathematics",
    },
    "science": {
        "keywords": "science",
        "display_name": "Science",
        "title_term": "science",
    },
    "reading": {
        "keywords": "reading",
        "display_name": "Reading",
        "title_term": "reading",
    },
}

# Gender configurations
GENDERS = {
    "both": {"title": "students", "subtitle": "students"},
    "boys": {"title": "boys", "subtitle": "boys"},
    "girls": {"title": "girls", "subtitle": "girls"},
}

# Dimension mapping configurations
SUBJECT_KEYWORDS = {config["keywords"]: key for key, config in SUBJECTS.items()}
SEX_KEYWORDS = {"all": "both", "boys": "boys", "girls": "girls", "boys_girls": "both"}


def run() -> None:
    """Main function to process PISA performance data and create collection."""
    # Load inputs
    config = paths.load_collection_config()
    ds = paths.load_dataset("pisa")
    tb = ds.read("pisa_math_boys_girls", load_data=False)

    # Filter PISA performance columns
    pisa_cols = get_pisa_performance_columns(tb)
    # Select only relevant columns
    tb = tb.loc[:, ["country", "year"] + pisa_cols].copy()

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
            "title": lambda view: generate_title_by_subject_and_gender(view),
            "subtitle": lambda view: generate_subtitle_by_subject_and_gender(view),
        }
    )

    # Save collection
    c.save()


def get_pisa_performance_columns(tb):
    """Filter PISA performance columns by subject and gender category."""
    pisa_cols = []

    # Look for PISA performance columns
    for col in tb.columns:
        if "pisa_" in col and "average" in col:
            # Include math, science, and reading performance columns
            if any(subject in col for subject in ["math", "science", "reading"]):
                pisa_cols.append(col)

    return pisa_cols


def adjust_dimensions(tb):
    """Add dimensions to PISA performance table columns."""

    def _extract_dimension(column_name, keyword_map):
        """Extract dimension value from column name using keyword mapping."""
        for keyword, value in keyword_map.items():
            if keyword in column_name:
                return value
        return None

    def _extract_gender(column_name):
        """Extract gender dimension from column name."""
        if "boys_girls" in column_name or "all" in column_name:
            return "both"
        elif "boys" in column_name:
            return "boys"
        elif "girls" in column_name:
            return "girls"
        return "both"  # Default

    # Process each column
    for col in tb.columns:
        if col in ["country", "year"]:
            continue

        # Extract subject and gender
        subject = _extract_dimension(col, SUBJECT_KEYWORDS)
        sex = _extract_gender(col)

        # Set indicator name
        tb[col].metadata.original_short_name = "pisa_performance"
        # Set dimensions
        tb[col].metadata.dimensions = {
            "subject": subject or "mathematics",
            "sex": sex or "both",
        }

    # Add dimension definitions to table metadata
    tb.metadata.dimensions.extend(
        [
            {"name": "Subject", "slug": "subject"},
            {"name": "Gender", "slug": "sex"},
        ]
    )

    return tb


def create_grouped_views(collection):
    """Add grouped views for gender and subject comparisons."""
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
                "view_config": view_config,
                "view_metadata": view_metadata,
            },
            {
                "dimension": "subject",
                "choice_new_slug": "subject_side_by_side",
                "choices": ["mathematics", "science", "reading"],
                "view_config": view_config,
                "view_metadata": view_metadata,
            },
        ],
        params={
            "title": lambda view: generate_title_by_subject_and_gender(view),
            "subtitle": lambda view: generate_subtitle_by_subject_and_gender(view),
        },
    )


# Common mappings used by both title and subtitle functions
GENDER_MAPPINGS = {
    "title": {"both": "students", "boys": "boys", "girls": "girls"},
    "subtitle": {"both": "students", "boys": "boys", "girls": "girls"},
}

SUBJECT_MAPPINGS = {
    "title": {
        "mathematics": "mathematics",
        "science": "science",
        "reading": "reading",
        "subject_side_by_side": "all subjects",
    },
    "subtitle": {
        "mathematics": "mathematics",
        "science": "science",
        "reading": "reading",
        "subject_side_by_side": "mathematics, science, and reading",
    },
}


def generate_title_by_subject_and_gender(view):
    """Generate title based on gender and subject."""
    sex, subject = view.dimensions["sex"], view.dimensions["subject"]

    # Get gender term
    gender_term = GENDER_MAPPINGS["title"].get(sex, "")
    # Get subject term
    subject_term = SUBJECT_MAPPINGS["title"].get(subject, "")

    if not subject_term:
        raise ValueError(f"Unknown subject: {subject}")

    if subject == "subject_side_by_side":
        return f"Average performance of 15-year-old {gender_term} by subject"
    else:
        return f"Average performance of 15-year-old {gender_term} in {subject_term}"


def generate_subtitle_by_subject_and_gender(view):
    """Generate subtitle based on subject and gender."""

    sex, subject = view.dimensions["sex"], view.dimensions["subject"]
    
    subject_term = SUBJECT_MAPPINGS["subtitle"].get(subject, "")
    gender_term = GENDER_MAPPINGS["subtitle"].get(sex, "")

    if not subject_term:
        raise ValueError(f"Unknown subject: {subject}")

    if subject == "subject_side_by_side":
        return f"Assessed through the PISA scales: mathematics, which evaluates problem-solving in real-life situations; science, which measures understanding and critical thinking about scientific issues; and reading, which gauges the ability to comprehend and use written information."
    else:
        return f"Average PISA scores in {subject_term} for {gender_term} aged 15. PISA is an international assessment that measures student performance in key subjects."

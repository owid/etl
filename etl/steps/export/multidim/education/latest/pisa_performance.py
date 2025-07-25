"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "yAxis": {"min": 350, "max": 550},
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

# Subject configurations
SUBJECTS = {
    "mathematics": {
        "keywords": "math",
        "display_name": "Mathematics",
        "title_term": "mathematics",
        "description": "Assessed through the PISA mathematics scale, which measures how well someone can use math to solve everyday problems and understand the role of math in the real world.",
    },
    "science": {
        "keywords": "science",
        "display_name": "Science",
        "title_term": "science",
        "description": "Assessed through the PISA science scale, which assesses how comfortable and knowledgeable someone is with science topics, focusing on their ability to discuss and think about scientific issues in everyday life.",
    },
    "reading": {
        "keywords": "reading",
        "display_name": "Reading",
        "title_term": "reading",
        "description": "Assessed through the PISA reading scale, which measures how well someone can understand and use written information to learn new things and be a part of society.",
    },
}

# Gender configurations
GENDERS = {
    "both": "students",
    "boys": "boys",
    "girls": "girls",
}

# Dimension mapping configurations
SUBJECT_KEYWORDS = {config["keywords"]: key for key, config in SUBJECTS.items()}
SEX_KEYWORDS = {"all": "both", "boys": "boys", "girls": "girls", "boys_girls": "both"}

# Constants
SUBJECT_SIDE_BY_SIDE_TITLE = "all subjects"
SUBJECT_SIDE_BY_SIDE_SUBTITLE = "mathematics, science, and reading"


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

    # Set global configuration
    c.set_global_config(
        config={
            "title": lambda view: generate_title_by_subject_and_gender(view),
            "subtitle": lambda view: generate_subtitle_by_subject_and_gender(view),
        }
    )

    # Edit display names
    for view in c.views:
        edit_indicator_displays(view)

    # Save collection
    c.save()


def get_pisa_performance_columns(tb):
    """Filter PISA performance columns by subject and gender category."""
    return [
        col
        for col in tb.columns
        if "pisa_" in col and "average" in col and any(subject in col for subject in ["math", "science", "reading"])
    ]


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


def generate_title_by_subject_and_gender(view):
    """Generate title based on gender and subject."""
    sex, subject = view.dimensions["sex"], view.dimensions["subject"]

    gender_term = GENDERS.get(sex, "students")

    if subject == "subject_side_by_side":
        return f"Average performance of 15-year-old {gender_term} by subject"

    subject_config = SUBJECTS.get(subject)
    if not subject_config:
        raise ValueError(f"Unknown subject: {subject}")

    return f"Average performance of 15-year-old {gender_term} in {subject_config['title_term']}"


def generate_subtitle_by_subject_and_gender(view):
    """Generate subtitle based on subject and gender."""
    sex, subject = view.dimensions["sex"], view.dimensions["subject"]

    gender_term = GENDERS.get(sex, "students")

    if subject == "subject_side_by_side":
        return f"Average scores in {SUBJECT_SIDE_BY_SIDE_SUBTITLE} for {gender_term} aged 15. Assessed through PISA scales, which evaluate children's ability to use mathematical reasoning, understand and engage with texts, and interact with scientific concepts for practical problem-solving, personal development, and informed citizenship."

    subject_config = SUBJECTS.get(subject)
    if not subject_config:
        raise ValueError(f"Unknown subject: {subject}")

    return (
        f"Average scores in {subject_config['title_term']} for {gender_term} aged 15. {subject_config['description']}"
    )


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.d.sex != "sex_side_by_side" or view.indicators.y is None:
        return

    DISPLAY_NAMES = {
        "average_girls": "Girls",
        "average_boys": "Boys",
    }

    for indicator in view.indicators.y:
        for gender_key, display_name in DISPLAY_NAMES.items():
            if gender_key in indicator.catalogPath:
                indicator.display = {"name": display_name}
                break

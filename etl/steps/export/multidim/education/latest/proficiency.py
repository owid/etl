"""Load a meadow dataset and create a garden dataset."""

import re

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLOR_PRIMARY = "#4C6A9C"
COLOR_LOWER_SECONDARY = "#883039"

COLOR_MAT = "#D73C50"
COLOR_READING = "#578145"

COLOR_BOYS = "#00847E"
COLOR_GIRLS = "#E56E5A"

COLOR_ALL_CHILDREN = "#B16214"
COLOR_STUDENTS = "#4C6A9C"

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

# Education level configurations
EDUCATION_LEVELS = {
    "primary": {
        "keywords": ["prepfuture_1", "primary"],
        "display_name": "Primary education",
        "title_term": "primary school age",
        "math_desc": "demonstrate skills in number sense and computation, basic measurement, reading, interpreting, and constructing graphs, spatial orientation, and number patterns",
        "reading_desc": "identify key ideas in texts and connect them to their own experiences",
    },
    "lower_secondary": {
        "keywords": ["prepfuture_2", "lowersec"],
        "display_name": "Lower secondary education",
        "title_term": "lower secondary school age",
        "math_desc": "demonstrate skills in computation, application problems, matching tables and graphs, and making use of algebraic representations",
        "reading_desc": "connect main ideas across different text types, understand the author's intentions, and draw conclusions from the text",
    },
}

# Subject configurations
SUBJECTS = {
    "mathematics": {
        "keywords": "math",
        "display_name": "Mathematics",
        "title_term": "mathematics",
    },
    "reading": {
        "keywords": "read",
        "display_name": "Reading",
        "title_term": "reading",
    },
}

# Gender configurations
GENDERS = {
    "both": "children",
    "male": "boys",
    "female": "girls",
    "sex_side_by_side": "boys and girls",
}

# Population configurations
POPULATIONS = {
    "all_children": {
        "title": "all children",
        "context": "This data accounts for school completion rates to estimate skills for all children, not just those in school.",
    },
    "students": {
        "title": "students",
        "context": "This data includes only students enrolled in school.",
    },
}

# Dimension mapping configurations
LEVEL_KEYWORDS = {}
for level_key, config in EDUCATION_LEVELS.items():
    for keyword in config["keywords"]:
        LEVEL_KEYWORDS[keyword] = level_key

SUBJECT_KEYWORDS = {config["keywords"]: key for key, config in SUBJECTS.items()}
SEX_KEYWORDS = {"both_sexes": "both", "male": "male", "female": "female"}

# DOD link mappings for education levels
LEVEL_DOD_LINKS = {
    "primary": "primary-education",
    "lower_secondary": "lower-secondary-education",
}

# Display name mappings for education levels
LEVEL_DISPLAY_NAMES = {
    "primary": "primary",
    "lower_secondary": "lower secondary",
}

# Exclusion patterns for column filtering
EXCLUSION_PATTERNS = [
    "urban",
    "rural",
    "the_global_age_specific_literacy_projections_model",
    "poorest_quintile",
    "richest_quintile",
    "adjusted",
    "very_affluent",
    "very_poor",
    "immigrant",
    "native",
    "language_of_the_test",
    "grade_2",
    "grade_3",
]


def run() -> None:
    """Main function to process prepared for the future data and create collection."""
    # Load inputs
    config = paths.load_collection_config()
    ds = paths.load_dataset("education_sdgs")
    tb = ds.read("education_sdgs", load_data=False)

    # Filter both prepared for the future and student proficiency columns
    proficiency_cols = get_proficiency_columns(tb)

    # Select only relevant columns
    tb = tb.loc[:, ["country", "year"] + proficiency_cols].copy()

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
            "title": lambda view: generate_title_by_dimensions(view),
            "subtitle": lambda view: generate_subtitle_by_dimensions(view),
        }
    )

    # Edit display names and set view metadata
    for view in c.views:
        level = view.dimensions["level"]
        sex = view.dimensions["sex"]
        population = view.dimensions["population"]
        subject = view.dimensions["subject"]

        if (
            level == "level_side_by_side"
            or sex == "sex_side_by_side"
            or population == "population_side_by_side"
            or subject == "subject_side_by_side"
        ):
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

    # Save collection
    c.save()


def get_proficiency_columns(tb):
    """Filter both prepared for the future and student proficiency columns excluding unwanted categories."""

    prepared_for_future_pattern = r"proportion_of_children_young_people.*prepared_for_the_future"
    student_proficiency_pattern = r"achieving_at_least_a_minimum_proficiency.*(mathematics|reading)"
    exclusion_pattern = "|".join(EXCLUSION_PATTERNS)

    return [
        col
        for col in tb.columns
        if (re.search(prepared_for_future_pattern, col) or re.search(student_proficiency_pattern, col))
        and not re.search(exclusion_pattern, col)
    ]


def adjust_dimensions(tb):
    """Add dimensions to prepared for the future table columns."""

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

    # Process each column
    for col in tb.columns:
        if col in ["country", "year"]:
            continue

        # Extract education level, subject, gender, and population type
        level = _extract_dimension(col, LEVEL_KEYWORDS)
        subject = _extract_dimension(col, SUBJECT_KEYWORDS)
        sex = _extract_gender(col, SEX_KEYWORDS)

        # Determine population type based on variable pattern
        if "prepared_for_the_future" in col:
            population = "all_children"
        elif "achieving_at_least_a_minimum_proficiency" in col:
            population = "students"
        else:
            population = "all_children"  # default

        # Set indicator name and dimensions
        tb[col].metadata.original_short_name = "minimum_proficiency"
        tb[col].metadata.dimensions = {
            "level": level or "primary",
            "subject": subject or "mathematics",
            "sex": sex or "both",
            "population": population,
        }

    # Add dimension definitions to table metadata
    tb.metadata.dimensions.extend(
        [
            {"name": "Education level", "slug": "level"},
            {"name": "Subject", "slug": "subject"},
            {"name": "Gender", "slug": "sex"},
            {"name": "Population", "slug": "population"},
        ]
    )

    return tb


def create_grouped_views(collection):
    """Add grouped views for gender, education level, and subject comparisons."""
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
                "choices": ["primary", "lower_secondary"],
                "view_config": view_config,
                "view_metadata": view_metadata,
            },
            {
                "dimension": "subject",
                "choice_new_slug": "subject_side_by_side",
                "choices": ["mathematics", "reading"],
                "view_config": view_config,
                "view_metadata": view_metadata,
            },
            {
                "dimension": "population",
                "choice_new_slug": "population_side_by_side",
                "choices": ["all_children", "students"],
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
    """Generate title based on gender, education level, subject, and population."""
    sex = view.dimensions.get("sex", "both")
    level = view.dimensions.get("level", "primary")
    subject = view.dimensions.get("subject", "mathematics")
    population = view.dimensions.get("population", "all_children")

    gender_term = GENDERS.get(sex, "children")
    level_config = EDUCATION_LEVELS.get(level, {})
    subject_config = SUBJECTS.get(subject, {})

    # Adjust gender term for population type
    if population == "students":
        gender_term = {"children": "students", "boys": "male students", "girls": "female students"}.get(
            gender_term, gender_term
        )

    level_term = level_config.get("title_term", level)
    subject_term = subject_config.get("title_term", subject)

    # Handle different combinations
    if view.matches(population="population_side_by_side"):
        return f"Share of {gender_term} of {level_term} achieving minimum proficiency in {subject_term}, by population type"
    elif view.matches(level="level_side_by_side"):
        if view.matches(subject="subject_side_by_side"):
            return f"Share of {gender_term} achieving minimum proficiency, by education level and subject"
        else:
            return f"Share of {gender_term} achieving minimum proficiency in {subject_term}, by education level"
    elif view.matches(subject="subject_side_by_side"):
        return f"Share of {gender_term} of {level_term} achieving minimum proficiency, by subject"
    elif view.matches(sex="sex_side_by_side"):
        return f"Share of children of {level_term} achieving minimum proficiency in {subject_term}, by gender"
    else:
        return f"Share of {gender_term} of {level_term} achieving minimum proficiency in {subject_term}"


def generate_subtitle_by_dimensions(view):
    """Generate subtitle based on dimensions."""
    level = view.dimensions.get("level", "primary")
    subject = view.dimensions.get("subject", "mathematics")
    population = view.dimensions.get("population", "all_children")

    level_config = EDUCATION_LEVELS.get(level, {})
    population_config = POPULATIONS.get(population, {})

    def get_proficiency_desc(subj):
        """Get proficiency description for subject and level."""
        if subj == "mathematics":
            return level_config.get("math_desc", "")
        else:
            return level_config.get("reading_desc", "")

    # Handle different view types
    if view.matches(level="level_side_by_side") and view.matches(subject="subject_side_by_side"):
        primary_math = EDUCATION_LEVELS["primary"]["math_desc"]
        secondary_math = EDUCATION_LEVELS["lower_secondary"]["math_desc"]
        primary_reading = EDUCATION_LEVELS["primary"]["reading_desc"]
        secondary_reading = EDUCATION_LEVELS["lower_secondary"]["reading_desc"]

        return f"The share of children who achieve the minimum [math](#dod:math-proficiency) and [reading](#dod:reading-proficiency) proficiency at different stages of education. For mathematics: at [primary](#dod:primary-education) level, students can {primary_math}; at [lower secondary](#dod:lower-secondary-education) level, students {secondary_math}. For reading: at primary level, children can {primary_reading}; at lower-secondary level, children can {secondary_reading}."

    elif view.matches(level="level_side_by_side"):
        desc_key = "math_desc" if subject == "mathematics" else "reading_desc"
        primary_desc = EDUCATION_LEVELS["primary"][desc_key]
        secondary_desc = EDUCATION_LEVELS["lower_secondary"][desc_key]
        subject_link = "math" if subject == "mathematics" else "reading"

        return f"The share of children who achieve minimum [{subject_link}](#dod:{subject_link}-proficiency) proficiency at different education levels. At [primary](#dod:primary-education) level, {'students' if subject == 'mathematics' else 'children'} can {primary_desc}. At [lower secondary](#dod:lower-secondary-education) level, {'students' if subject == 'mathematics' else 'children'} can {secondary_desc}."

    elif view.matches(subject="subject_side_by_side"):
        math_desc = level_config.get("math_desc", "")
        reading_desc = level_config.get("reading_desc", "")

        level_dod = LEVEL_DOD_LINKS.get(level, f"{level}-education")
        level_display = LEVEL_DISPLAY_NAMES.get(level, level)
        return f"The share of children who achieve minimum proficiency by the end of [{level_display}](#dod:{level_dod}) education. For [math](#dod:math-proficiency), students can {math_desc}. For [reading](#dod:reading-proficiency), children can {reading_desc}."

    elif view.matches(population="population_side_by_side"):
        proficiency_desc = get_proficiency_desc(subject)
        subject_link = "math" if subject == "mathematics" else "reading"

        level_dod = LEVEL_DOD_LINKS.get(level, f"{level}-education")
        level_display = LEVEL_DISPLAY_NAMES.get(level, level)
        return f"The share of children who achieve minimum [{subject_link}](#dod:{subject_link}-proficiency) proficiency by the end of [{level_display}](#dod:{level_dod}) education, where {'students' if subject == 'mathematics' else 'children'} can {proficiency_desc}. Compares all children in the age group versus only those enrolled in school."

    else:
        proficiency_desc = get_proficiency_desc(subject)
        subject_link = "math" if subject == "mathematics" else "reading"
        population_context = population_config.get("context", "")

        level_dod = LEVEL_DOD_LINKS.get(level, f"{level}-education")
        level_display = LEVEL_DISPLAY_NAMES.get(level, level)
        return f"The share of children who achieve minimum [{subject_link}](#dod:{subject_link}-proficiency) proficiency by the end of [{level_display}](#dod:{level_dod}) education, where {'students' if subject == 'mathematics' else 'children'} can {proficiency_desc}. {population_context}"


def edit_indicator_displays(view):
    """Edit display names and colors for the grouped views."""
    if view.indicators.y is None:
        return

    display_config = {
        "level": {
            "primary": {"name": "Primary education", "color": COLOR_PRIMARY},
            "lower_secondary": {"name": "Lower secondary education", "color": COLOR_LOWER_SECONDARY},
        },
        "subject": {
            "mathematics": {"name": "Mathematics", "color": COLOR_MAT},
            "reading": {"name": "Reading", "color": COLOR_READING},
        },
        "gender": {
            "_male": {"name": "Boys", "color": COLOR_BOYS},
            "_female": {"name": "Girls", "color": COLOR_GIRLS},
        },
        "population": {
            "all_children": {"name": "All children", "color": COLOR_ALL_CHILDREN},
            "students": {"name": "Students", "color": COLOR_STUDENTS},
        },
    }

    for indicator in view.indicators.y:
        if view.matches(population="population_side_by_side"):
            for pop_key, config in display_config["population"].items():
                if (pop_key == "all_children" and "prepared_for_the_future" in indicator.catalogPath) or (
                    pop_key == "students" and "achieving_at_least_a_minimum_proficiency" in indicator.catalogPath
                ):
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break
        elif view.matches(level="level_side_by_side"):
            for level_key, config in display_config["level"].items():
                if level_key in indicator.catalogPath:
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

        elif view.matches(subject="subject_side_by_side"):
            for subject_key, config in display_config["subject"].items():
                if subject_key in indicator.catalogPath:
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break
        elif view.matches(sex="sex_side_by_side"):
            for gender_key, config in display_config["gender"].items():
                if gender_key in indicator.catalogPath:
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

    # Sort indicators for level_side_by_side: primary → lower secondary
    if view.matches(level="level_side_by_side"):
        def get_level_index(ind):
            if "primary" in ind.catalogPath and "lower" not in ind.catalogPath:
                return 0
            elif "lower_secondary" in ind.catalogPath:
                return 1
            return 2

        view.indicators.y = sorted(view.indicators.y, key=get_level_index)

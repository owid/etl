"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "yAxis": {"min": 0, "max": 100},
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

# Education level configurations - maps internal level keys to display formatting
EDUCATION_LEVELS = {
    "primary": {
        "keywords": ["prepfuture_1", "primary"],
        "display_name": "Primary education",
        "age_description": "at the age of primary education",
        "title_term": "primary school age",
    },
    "lower_secondary": {
        "keywords": ["prepfuture_2", "lowersec"],
        "display_name": "Lower secondary education",
        "age_description": "at the age of lower secondary education",
        "title_term": "lower secondary school age",
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
    "both": {"title": "children", "subtitle": "children"},
    "male": {"title": "boys", "subtitle": "boys"},
    "female": {"title": "girls", "subtitle": "girls"},
    "sex_side_by_side": {"title": "boys and girls", "subtitle": "boys and girls"},
}

# Population configurations
POPULATIONS = {
    "all_children": {
        "title": "all children",
        "subtitle": "all children",
        "description": "includes all children in the relevant age group, not just those enrolled in school",
    },
    "students": {
        "title": "students",
        "subtitle": "students",
        "description": "includes only students enrolled in school",
    },
}

# Dimension mapping configurations - flattened for multiple keywords per level
LEVEL_KEYWORDS = {}
for level_key, config in EDUCATION_LEVELS.items():
    for keyword in config["keywords"]:
        LEVEL_KEYWORDS[keyword] = level_key

SUBJECT_KEYWORDS = {config["keywords"]: key for key, config in SUBJECTS.items()}
SEX_KEYWORDS = {"both_sexes": "both", "male": "male", "female": "female"}


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

    # Edit FAUST
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


def get_proficiency_columns(tb):
    """Filter both prepared for the future and student proficiency columns excluding unwanted categories."""
    # Create inclusion patterns for both variable types
    prepared_for_future_pattern = r"proportion_of_children_young_people.*prepared_for_the_future"
    student_proficiency_pattern = r"achieving_at_least_a_minimum_proficiency.*(mathematics|reading)"

    # Create exclusion pattern - filters out unwanted column types:
    # - Geographic breakdowns (urban/rural)
    # - Wealth quintile data (poorest/richest, very_affluent/very_poor)
    # - Model-specific projections
    # - Adjusted parity indices (except basic ones)
    # - Language and immigration status
    # - Specific grade levels (grade_2, grade_3) as the coverage is pretty low
    exclusion_pattern = "|".join(
        [
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
    )

    # Filter columns using regex
    import re

    proficiency_cols = [
        col
        for col in tb.columns
        if (re.search(prepared_for_future_pattern, col) or re.search(student_proficiency_pattern, col))
        and not re.search(exclusion_pattern, col)
    ]
    return proficiency_cols


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

    # Get terms
    gender_term = GENDERS.get(sex, {}).get("title", "children")
    level_config = EDUCATION_LEVELS.get(level, {})
    subject_config = SUBJECTS.get(subject, {})

    # Adjust gender term for population type
    if population == "students":
        if gender_term == "children":
            gender_term = "students"
        elif gender_term == "boys":
            gender_term = "male students"
        elif gender_term == "girls":
            gender_term = "female students"

    # Handle different combinations
    if view.matches(population="population_side_by_side"):
        return f"Share of {gender_term} of {level_config.get('title_term', level)} achieving minimum proficiency in {subject_config.get('title_term', subject)}, by population type"
    elif view.matches(level="level_side_by_side"):
        if view.matches(subject="subject_side_by_side"):
            return f"Share of {gender_term} achieving minimum proficiency, by education level and subject"
        else:
            return f"Share of {gender_term} achieving minimum proficiency in {subject_config.get('title_term', subject)}, by education level"
    elif view.matches(subject="subject_side_by_side"):
        return f"Share of {gender_term} of {level_config.get('title_term', level)} achieving minimum proficiency, by subject"
    elif view.matches(sex="sex_side_by_side"):
        return f"Share of children of {level_config.get('title_term', level)} achieving minimum proficiency in {subject_config.get('title_term', subject)}, by gender"
    else:
        return f"Share of {gender_term} of {level_config.get('title_term', level)} achieving minimum proficiency in {subject_config.get('title_term', subject)}"


def generate_subtitle_by_dimensions(view):
    """Generate subtitle based on dimensions."""
    level = view.dimensions.get("level", "primary")
    subject = view.dimensions.get("subject", "mathematics")
    population = view.dimensions.get("population", "all_children")

    # Define proficiency descriptions
    math_primary_desc = "demonstrate skills in number sense and computation, basic measurement, reading, interpreting, and constructing graphs, spatial orientation, and number patterns"
    math_secondary_desc = "demonstrate skills in computation, application problems, matching tables and graphs, and making use of algebraic representations"
    reading_primary_desc = "identify key ideas in texts and connect them to their own experiences"
    reading_secondary_desc = "connect main ideas across different text types, understand the author's intentions, and draw conclusions from the text"

    # Population context descriptions
    all_children_context = "This data accounts for school completion rates to estimate skills for all children, not just those in school."
    students_context = "This data includes only students enrolled in school."

    # Handle different view types
    if view.matches(level="level_side_by_side") and view.matches(subject="subject_side_by_side"):
        # Both levels and subjects side by side
        return "The share of children who achieve the minimum [math](#dod:math-proficiency) and [reading](#dod:reading-proficiency) proficiency at different stages of education. For mathematics: at [primary](#dod:primary-education) level, students can demonstrate skills in number sense and computation, basic measurement, reading, interpreting, and constructing graphs, spatial orientation, and number patterns; at [lower-secondary](#dod:lower-secondary-education) level, students demonstrate skills in computation, application problems, matching tables and graphs, and making use of algebraic representations. For reading: at primary level, children can identify key ideas in texts and connect them to their own experiences; at lower-secondary level, children can connect main ideas across different text types, understand the author's intentions, and draw conclusions from the text."
    elif view.matches(level="level_side_by_side"):
        # Multiple education levels
        if subject == "mathematics":
            return f"The share of children who achieve minimum [math](#dod:math-proficiency) proficiency at different education levels. At [primary](#dod:primary-education) level, students can {math_primary_desc}. At [lower-secondary](#dod:lower-secondary-education) level, students {math_secondary_desc}."
        else:
            return f"The share of children who achieve minimum [reading](#dod:reading-proficiency) proficiency at different education levels. At [primary](#dod:primary-education) level, children can {reading_primary_desc}. At [lower-secondary](#dod:lower-secondary-education) level, children can {reading_secondary_desc}."
    elif view.matches(subject="subject_side_by_side"):
        # Multiple subjects
        if level == "primary":
            return f"The share of children who achieve minimum proficiency by the end of [primary](#dod:primary-education) education. For [mathematics](#dod:math-proficiency), students can {math_primary_desc}. For [reading](#dod:reading-proficiency), children can {reading_primary_desc}."
        else:
            return f"The share of children who achieve minimum proficiency by the end of [lower-secondary](#dod:lower-secondary-education) education. For [mathematics](#dod:math-proficiency), students {math_secondary_desc}. For [reading](#dod:reading-proficiency), children can {reading_secondary_desc}."
    elif view.matches(population="population_side_by_side"):
        # Different populations
        if subject == "mathematics":
            if level == "primary":
                proficiency_desc = f"can {math_primary_desc}"
            else:
                proficiency_desc = math_secondary_desc
            return f"The share of children who achieve minimum [math](#dod:math-proficiency) proficiency by the end of [{level}](#dod:{level}-education) education, where students {proficiency_desc}. Compares all children in the age group versus only those enrolled in school."
        else:
            if level == "primary":
                proficiency_desc = f"can {reading_primary_desc}"
            else:
                proficiency_desc = f"can {reading_secondary_desc}"
            return f"The share of children who achieve minimum [reading](#dod:reading-proficiency) proficiency by the end of [{level}](#dod:{level}-education) education, where children {proficiency_desc}. Compares all children in the age group versus only those enrolled in school."
    else:
        # Single dimension view
        if subject == "mathematics":
            if level == "primary":
                proficiency_desc = f"can {math_primary_desc}"
            else:
                proficiency_desc = math_secondary_desc
            population_context = all_children_context if population == "all_children" else students_context
            return f"The share of children who achieve minimum [math](#dod:math-proficiency) proficiency by the end of [{level}](#dod:{level}-education) education, where students {proficiency_desc}. {population_context}"
        else:
            if level == "primary":
                proficiency_desc = f"can {reading_primary_desc}"
            else:
                proficiency_desc = f"can {reading_secondary_desc}"
            population_context = all_children_context if population == "all_children" else students_context
            return f"The share of children who achieve minimum [reading](#dod:reading-proficiency) proficiency by the end of [{level}](#dod:{level}-education) education, where children {proficiency_desc}. {population_context}"


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.indicators.y is None:
        return

    # Display name mappings
    level_display_names = {
        "primary": "Primary education",
        "lower_secondary": "Lower secondary education",
    }

    subject_display_names = {
        "mathematics": "Mathematics",
        "reading": "Reading",
    }

    gender_display_names = {
        "both_sexes": "Both genders",
        "_male": "Boys",
        "_female": "Girls",
    }

    population_display_names = {
        "all_children": "All children",
        "students": "Students",
    }

    for indicator in view.indicators.y:
        # Check for population-based display names
        if view.matches(population="population_side_by_side"):
            for pop_key, display_name in population_display_names.items():
                if (pop_key == "all_children" and "prepared_for_the_future" in indicator.catalogPath) or (
                    pop_key == "students" and "achieving_at_least_a_minimum_proficiency" in indicator.catalogPath
                ):
                    indicator.display = {"name": display_name}
                    break

        # Check for level-based display names
        elif view.matches(level="level_side_by_side"):
            for level_key, display_name in level_display_names.items():
                if level_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break

        # Check for subject-based display names
        elif view.matches(subject="subject_side_by_side"):
            for subject_key, display_name in subject_display_names.items():
                if subject_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break

        # Check for gender-based display names
        elif view.matches(sex="sex_side_by_side"):
            for gender_key, display_name in gender_display_names.items():
                if gender_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break

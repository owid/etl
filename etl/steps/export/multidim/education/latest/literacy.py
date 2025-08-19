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
    "addCountryMode": "add-country",
}

# Common grouped view configuration
GROUPED_VIEW_CONFIG = MULTIDIM_CONFIG | {
    "hasMapTab": False,
    "tab": "chart",
    "selectedFacetStrategy": "entity",
}

# Age group configurations - maps internal age group keys to column detection and display formatting
# - keywords: column name substring used to identify this age group in source data
# - age_range: human-readable age range for subtitles
# - age_short: abbreviated age range for chart legends
# - title_term: noun used in chart titles (e.g. "adults", "young people")
# - gender_prefix: prefix added when combining with gender terms (e.g. "young men")
AGE_GROUPS = {
    "adult": {
        "keywords": "population_15plus_years",
        "age_range": "15 years and older",
        "age_short": "15+",
        "title_term": "adults",
        "gender_prefix": "",
    },
    "youth": {
        "keywords": "population_15_24_years",
        "age_range": "15 to 24 years",
        "age_short": "15–24",
        "title_term": "young people",
        "gender_prefix": "young ",
    },
    "elderly": {
        "keywords": "population_65plus_years",
        "age_range": "65 years and older",
        "age_short": "65+",
        "title_term": "older adults",
        "gender_prefix": "older ",
    },
}

# Gender configurations - maps internal gender keys to display text for chart titles and subtitles
# Used in chart title generation and gender-specific visualizations
# - title: noun used in chart titles
# - subtitle: noun used in chart subtitles
GENDERS = {
    "both": {"title": "adults", "subtitle": "adults"},
    "male": {"title": "men", "subtitle": "men"},
    "female": {"title": "women", "subtitle": "women"},
    "sex_side_by_side": {"title": "men and women", "subtitle": "men and women"},
}

# Dimension mapping configurations - reverse lookups for extracting dimensions from column names
# AGE_GROUP_KEYWORDS: maps column keywords (e.g. "population_15plus_years") -> age group keys (e.g. "adult")
# SEX_KEYWORDS: maps column sex terms (e.g. "both_sexes", "male") -> internal gender keys (e.g. "both", "male")
AGE_GROUP_KEYWORDS = {config["keywords"]: key for key, config in AGE_GROUPS.items()}
SEX_KEYWORDS = {"both_sexes": "both", "male": "male", "female": "female"}


def run() -> None:
    """Main function to process literacy rates data and create collection."""
    # Load inputs
    config = paths.load_collection_config()
    ds = paths.load_dataset("education_sdgs")
    tb = ds.read("education_sdgs", load_data=False)

    # Filter literacy rate columns
    literacy_cols = get_literacy_rate_columns(tb)

    # Select only relevant columns
    tb = tb.loc[:, ["country", "year"] + literacy_cols].copy()

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
            "title": lambda view: generate_title_by_gender_and_age(view),
            "subtitle": lambda view: generate_subtitle_by_age_and_gender(view),
        }
    )

    # Edit display names
    for view in c.views:
        edit_indicator_displays(view)

    # Save collection
    c.save()


def get_literacy_rate_columns(tb):
    """Filter literacy rate columns by age and gender category."""
    # Create a regex pattern that matches literacy rate columns
    literacy_patterns = "|".join(
        [
            r"adult_literacy_rate.*population_15plus_years",
            r"youth_literacy_rate.*population_15_24_years",
            r"elderly_literacy_rate.*population_65plus_years",
        ]
    )

    # Create exclusion pattern - filters out unwanted column types:
    # - Geographic breakdowns (urban/rural)
    # - Wealth quintile data (poorest/richest)
    # - Model-specific projections
    # - Adjusted parity index
    exclusion_pattern = "|".join(
        [
            "urban",
            "rural",
            "the_global_age_specific_literacy_projections_model",
            "poorest_quintile",
            "richest_quintile",
            "adjusted",
        ]
    )

    # Filter columns using regex
    import re

    literacy_cols = [
        col for col in tb.columns if re.search(literacy_patterns, col) and not re.search(exclusion_pattern, col)
    ]
    return literacy_cols


def adjust_dimensions(tb):
    """Add dimensions to literacy rates table columns."""

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

        # Extract age group and gender
        age_group = _extract_dimension(col, AGE_GROUP_KEYWORDS)
        sex = _extract_gender(col, SEX_KEYWORDS)

        # Set indicator name and dimensions
        tb[col].metadata.original_short_name = "literacy_rates"
        tb[col].metadata.dimensions = {
            "age_group": age_group or "adult",
            "sex": sex or "both",
        }

    # Add dimension definitions to table metadata
    tb.metadata.dimensions.extend(
        [
            {"name": "Age group", "slug": "age_group"},
            {"name": "Gender", "slug": "sex"},
        ]
    )

    return tb


def create_grouped_views(collection):
    """Add grouped views for gender and age group comparisons."""
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
                "dimension": "age_group",
                "choice_new_slug": "age_side_by_side",
                "choices": {"adult", "youth", "elderly"},
                "view_config": view_config,
                "view_metadata": view_metadata,
            },
        ],
        params={
            "title": lambda view: generate_title_by_gender_and_age(view),
            "subtitle": lambda view: generate_subtitle_by_age_and_gender(view),
        },
    )


def generate_title_by_gender_and_age(view):
    """Generate title based on gender and age group."""
    sex, age_group = view.dimensions["sex"], view.dimensions["age_group"]

    # Get gender and age terms
    gender_term = GENDERS.get(sex, {}).get("title", "")

    if view.matches(age_group="age_side_by_side"):
        term = "by age group"
    elif view.matches(sex="sex_side_by_side"):
        age_config = AGE_GROUPS.get(age_group, {})
        term = f"among {age_config.get('title_term', age_group)}, by gender"
    elif view.matches(sex="both"):
        age_config = AGE_GROUPS.get(age_group, {})
        term = f"among {age_config.get('title_term', age_group)}"
    else:
        # For specific genders, create better titles
        age_config = AGE_GROUPS.get(age_group, {})
        prefix = age_config.get("gender_prefix", "")
        term = f"among {prefix}{gender_term}"

    return f"Literacy rates {term}"


def _get_age_descriptions_by_gender(sex):
    """Get age descriptions formatted for specific gender."""
    if sex == "both":
        return "adults aged 15 and above, young people (15–24), and older adults (65 and above)"
    elif sex == "male":
        return "men aged 15 and above, young men (15–24), and older men (65 and above)"
    elif sex == "female":
        return "women aged 15 and above, young women (15–24), and older women (65 and above)"
    else:
        return "adults, young people, and older adults"


def _get_gender_specific_subtitle(age_group, gender_term):
    """Get gender-specific subtitle for specific age groups."""
    age_config = AGE_GROUPS.get(age_group, {})
    prefix = age_config.get("gender_prefix", "")
    age_range = age_config.get("age_range", "")

    return f"Share of {prefix}{gender_term} aged {age_range} who can read and write a simple sentence about their daily life."


def _get_sex_side_by_side_subtitle(age_group):
    """Get subtitle for gender side-by-side comparisons."""
    age_config = AGE_GROUPS.get(age_group, {})
    prefix = age_config.get("gender_prefix", "")
    age_range = age_config.get("age_range", "")

    if prefix:
        return f"Share of {prefix}women and men aged {age_range} who can read and write a simple sentence about their daily life."
    else:
        return (
            f"Share of women and men aged {age_range} who can read and write a simple sentence about their daily life."
        )


def generate_subtitle_by_age_and_gender(view):
    """Generate subtitle based on age group and gender."""
    sex, age_group = view.dimensions["sex"], view.dimensions["age_group"]

    # Handle different combinations
    if age_group == "age_side_by_side":
        age_descriptions = _get_age_descriptions_by_gender(sex)
        if age_descriptions:
            return f"Share of {age_descriptions} who can read and write a simple sentence about their daily life."
        else:
            return "Share of adults, young people, and older adults who can read and write a simple sentence about their daily life."

    elif sex == "sex_side_by_side":
        return _get_sex_side_by_side_subtitle(age_group)

    elif sex == "both":
        age_config = AGE_GROUPS.get(age_group, {})
        age_range = age_config.get("age_range", "")
        return f"Share of {age_config.get('title_term', 'adults')} aged {age_range} who can read and write a simple sentence about their daily life."

    else:
        # For specific genders
        gender_term = GENDERS.get(sex, {}).get("subtitle", "")
        return _get_gender_specific_subtitle(age_group, gender_term)


def _get_age_display_names(sex):
    """Get age display names based on gender."""
    term = {
        "male": "Men",
        "female": "Women",
    }.get(sex, "Adults")

    return {
        "adult": f"{term} ({AGE_GROUPS['adult']['age_short']})",
        "youth": f"Young {term.lower()} ({AGE_GROUPS['youth']['age_short']})",
        "elderly": f"Older {term.lower()} ({AGE_GROUPS['elderly']['age_short']})",
    }


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.indicators.y is None:
        return

    sex = view.d.sex

    # Get appropriate display names
    age_display_names = _get_age_display_names(sex)
    gender_display_names = {
        "years__male": "Men",
        "years__female": "Women",
        "both": "Both genders",
    }

    for indicator in view.indicators.y:
        # Check for age-based display names
        if view.d.age_group == "age_side_by_side":
            for age_key, display_name in age_display_names.items():
                if age_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break

        # Check for gender-based display names
        elif sex == "sex_side_by_side":
            for gender_key, display_name in gender_display_names.items():
                if gender_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break

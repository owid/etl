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

# Column filtering patterns
LITERACY_RATE_PATTERNS = {
    "adult": ["adult_literacy_rate", "population_15plus_years", "both_sexes"],
    "adult_male": ["adult_literacy_rate", "population_15plus_years", "years__male"],
    "adult_female": ["adult_literacy_rate", "population_15plus_years", "years__female"],
    "youth_both": ["youth_literacy_rate", "population_15_24_years", "both_sexes"],
    "youth_male": ["youth_literacy_rate", "population_15_24_years", "years__male"],
    "youth_female": ["youth_literacy_rate", "population_15_24_years", "years__female"],
    "elderly": ["elderly_literacy_rate", "population_65plus_years", "both_sexes"],
    "elderly_male": ["elderly_literacy_rate", "population_65plus_years", "years__male"],
    "elderly_female": ["elderly_literacy_rate", "population_65plus_years", "years__female"],
}


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
    literacy_cols = []

    # Exclusion patterns
    exclusion_patterns = [
        "urban",
        "rural",
        "the_global_age_specific_literacy_projections_model",
        "poorest_quintile",
        "richest_quintile",
        "adjusted",
    ]

    for patterns in LITERACY_RATE_PATTERNS.values():
        cols = [col for col in tb.columns if all(pattern in col for pattern in patterns)]
        # Filter out columns containing exclusion patterns
        cols = [col for col in cols if not any(exclusion in col for exclusion in exclusion_patterns)]
        literacy_cols.extend(cols)

    return literacy_cols


def adjust_dimensions(tb):
    """Add dimensions to literacy rates table columns."""

    # Auxiliary functions just used in this function
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

    # Dimension mapping configurations
    AGE_GROUP_KEYWORDS = {
        "population_15plus_years": "adult",
        "population_15_24_years": "youth",
        "population_65plus_years": "elderly",
    }

    SEX_KEYWORDS = {"both_sexes": "both", "male": "male", "female": "female"}

    # Process each column
    for col in tb.columns:
        if col in ["country", "year"]:
            continue

        # Extract age group
        age_group = _extract_dimension(col, AGE_GROUP_KEYWORDS)

        # Extract gender
        sex = _extract_gender(col, SEX_KEYWORDS)

        # Set indicator name
        tb[col].metadata.original_short_name = "literacy_rates"

        # Set dimensions
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


# Common mappings used by both title and subtitle functions
GENDER_MAPPINGS = {
    "title": {
        "both": "adults",
        "male": "men",
        "female": "women",
        "sex_side_by_side": "men and women",
    },
    "subtitle": {
        "both": "adults",
        "male": "men",
        "female": "women",
        "sex_side_by_side": "men and women",
    },
}

AGE_GROUP_MAPPINGS = {
    "title": {
        "adult": "adults",
        "youth": "young people",
        "elderly": "older adults",
        "age_side_by_side": "by age group",
    },
    "subtitle": {
        "adult": "adults aged 15 years and older",
        "youth": "young people aged 15 to 24 years",
        "elderly": "older adults aged 65 years and older",
        "age_side_by_side": "adults (15+), young people (15-24), and older adults (65+)",
    },
}


def generate_title_by_gender_and_age(view):
    """Generate title based on gender and age group."""
    sex, age_group = view.dimensions["sex"], view.dimensions["age_group"]

    # Get gender term
    gender_term = GENDER_MAPPINGS["title"].get(sex, "")
    # Get age group term
    age_term = AGE_GROUP_MAPPINGS["title"].get(age_group, "")

    if not age_term:
        raise ValueError(f"Unknown age group: {age_group}")

    if age_group == "age_side_by_side":
        return f"Literacy rates {age_term}"
    elif sex == "sex_side_by_side":
        return f"Literacy rates among {age_term}, by gender"
    elif sex == "both":
        return f"Literacy rates among {age_term}"
    else:
        # For specific genders, create better titles
        if age_group == "youth":
            return f"Literacy rates among young {gender_term}"
        elif age_group == "elderly":
            return f"Literacy rates among older {gender_term}"
        elif age_group == "adult":
            return f"Literacy rates among {gender_term}"
        else:
            return f"Literacy rates among {gender_term} {age_term}"


def generate_subtitle_by_age_and_gender(view):
    """Generate subtitle based on age group and gender with links."""
    sex, age_group = view.dimensions["sex"], view.dimensions["age_group"]

    age_term = AGE_GROUP_MAPPINGS["subtitle"].get(age_group, "")
    gender_term = GENDER_MAPPINGS["subtitle"].get(sex, "")

    if not age_term:
        raise ValueError(f"Unknown age group: {age_group}")

    # Handle different combinations properly
    if age_group == "age_side_by_side":
        # Generate gender-specific age descriptions
        if sex == "both":
            age_descriptions = "adults aged 15 and above, young people (15–24), and older adults (65 and above)"
        elif sex == "male":
            age_descriptions = "men aged 15 and above, young men (15–24), and older men (65 and above)"
        elif sex == "female":
            age_descriptions = "women aged 15 and above, young women (15–24), and older women (65 and above)"
        else:
            age_descriptions = age_term  # fallback to original
        return f"Share of {age_descriptions} who can read and write a short, simple sentence with understanding."
    elif sex == "sex_side_by_side":
        # Generate gender-specific subtitle based on age group
        if age_group == "adult":
            return "Share of women and men aged 15 years and older who can read and write a short, simple sentence with understanding."
        elif age_group == "youth":
            return "Share of young women and men aged 15 to 24 years who can read and write a short, simple sentence with understanding."
        elif age_group == "elderly":
            return "Share of older women and men aged 65 years and older who can read and write a short, simple sentence with understanding."
        else:
            return f"Share of women and men among {age_term} who can read and write a short, simple sentence with understanding."
    elif sex == "both":
        return f"Share of {age_term} who can read and write a short, simple sentence with understanding."
    else:
        # For specific genders, create age-specific phrasing
        if age_group == "youth":
            return f"Share of young {gender_term} aged 15 to 24 years who can read and write a short, simple sentence with understanding."
        elif age_group == "elderly":
            return f"Share of older {gender_term} aged 65 years and older who can read and write a short, simple sentence with understanding."
        elif age_group == "adult":
            return f"Share of {gender_term} aged 15 years and older who can read and write a short, simple sentence with understanding."
        else:
            return f"Share of {gender_term} among {age_term} who can read and write a short, simple sentence with understanding."


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.indicators.y is None:
        return

    # Generate gender-specific age display names
    sex = view.dimensions.get("sex", "both")
    if sex == "male":
        AGE_DISPLAY_NAMES = {
            "adult": "Men (15+)",
            "youth": "Young men (15–24)",
            "elderly": "Older men (65+)",
        }
    elif sex == "female":
        AGE_DISPLAY_NAMES = {
            "adult": "Women (15+)",
            "youth": "Young women (15–24)",
            "elderly": "Older women (65+)",
        }
    else:  # both sexes
        AGE_DISPLAY_NAMES = {
            "adult": "Adults (15+)",
            "youth": "Young people (15–24)",
            "elderly": "Older adults (65+)",
        }

    # Display name mappings for gender
    GENDER_DISPLAY_NAMES = {
        "years__male": "Men",
        "years__female": "Women",
        "both": "Both genders",
    }

    for indicator in view.indicators.y:
        display_name = None

        # Check for age-based display names
        if view.dimensions.get("age_group") == "age_side_by_side":
            for age_key, display_name in AGE_DISPLAY_NAMES.items():
                if age_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break

        # Check for gender-based display names
        elif view.dimensions.get("sex") == "sex_side_by_side":
            for gender_key, display_name in GENDER_DISPLAY_NAMES.items():
                if gender_key in indicator.catalogPath:
                    indicator.display = {"name": display_name}
                    break

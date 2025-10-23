"""Load a meadow dataset and create a garden dataset."""

from etl.collection import combine_collections
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Color constants for education levels and gender
COLOR_PREPRIMARY = "#D73C50"
COLOR_PRIMARY = "#4C6A9C"
COLOR_SECONDARY = "#578145"
COLOR_TERTIARY = "#B16214"
COLOR_ALL_LEVELS = "#6C7B7F"  # Gray color for "All levels"

COLOR_BOYS = "#00847E"
COLOR_GIRLS = "#E56E5A"

# Color constants for metric types
COLOR_EXPECTED_YEARS = "#4C6A9C"  # Blue
COLOR_AVERAGE_YEARS = "#B16214"  # Orange
COLOR_LEARNING_ADJUSTED = "#578145"  # Green

# Used in metric type side-by-side comparison views
METRIC_TYPE_DESCRIPTION_KEY = [
    "These three measures capture different aspects of educational participation and achievement across the lifespan.",
    "**Average years of schooling** measures the actual educational attainment of adults aged 25 and older - the average number of years they have completed in formal education. This reflects past educational outcomes.",
    "**Expected years of schooling** shows how many years a child starting school can expect to spend in education if current enrollment patterns continue. This is a forward-looking measure based on current school participation rates.",
    "**Learning-adjusted years of schooling** combines quantity and quality of education, adjusting expected years by learning outcomes from international assessments. It accounts for the fact that similar years of schooling can yield different learning results.",
    "Together, these three numbers show us: how many years of education today's adults completed (average), how many years we expect new students to spend in school (expected), and how many years of schooling new students will get when we adjust those years for how much they actually learn (learning-adjusted).",
    "Data comes from administrative school records, household surveys, and international learning assessments.",
]


MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "hasMapTab": True,
    "tab": "map",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "add-country",
}

# Common mappings for title and subtitle generation
GENDER_MAPPINGS = {
    "title": {
        "both": "children",
        "boys": "boys",
        "girls": "girls",
        "sex_side_by_side": "girls and boys",
        "metric_type_side_by_side": "children",
    },
    "subtitle": {
        "both": "children",
        "boys": "boys",
        "girls": "girls",
        "sex_side_by_side": "boys and girls",
        "metric_type_side_by_side": "children",
    },
    "tertiary": {
        "both": "young people",
        "boys": "men",
        "girls": "women",
        "sex_side_by_side": "men and women",
        "metric_type_side_by_side": "young people",
    },
    "average_years_schooling": {
        "both": "adults",
        "boys": "men",
        "girls": "women",
        "sex_side_by_side": "men and women",
        "metric_type_side_by_side": "adults",
    },
    "learning_adjusted_years_schooling": {
        "both": "children",
        "boys": "boys",
        "girls": "girls",
        "sex_side_by_side": "boys and girls",
        "metric_type_side_by_side": "children",
    },
}

LEVEL_MAPPINGS = {
    "title": {
        "primary": "primary education",
        "preprimary": "pre-primary education",
        "secondary": "secondary education",
        "tertiary": "tertiary education",
        "all": "all education levels",
        "level_side_by_side": "education",
        "metric_type_side_by_side": "education",
    },
    "subtitle": {
        "primary": "[primary](#dod:primary-education)",
        "preprimary": "[pre-primary](#dod:pre-primary-education)",
        "secondary": "[secondary](#dod:secondary-education)",
        "tertiary": "[tertiary](#dod:tertiary-education)",
        "all": "all education levels",
        "level_side_by_side": "[pre-primary](#dod:pre-primary-education), [primary](#dod:primary-education), [secondary](#dod:secondary-education), and [tertiary](#dod:tertiary-education)",
    },
}

METRIC_MAPPINGS = {
    "expected_years_schooling": "Expected years of schooling",
    "average_years_schooling": "Average years of schooling",
    "learning_adjusted_years_schooling": "Learning-adjusted years of schooling",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load datasets
    ds_undp = paths.load_dataset("undp_hdr")
    tb_undp = ds_undp.read("undp_hdr_sex")

    ds_opri = paths.load_dataset("education_opri")
    tb_opri = ds_opri.read("education_opri")

    ds_gender_stats = paths.load_dataset("gender_statistics")
    tb_gender_stats = ds_gender_stats.read("gender_statistics")

    # UNDP expected years of schooling columns
    cols_undp = tb_undp.filter(regex=r"(eys|mys)__sex_").columns
    assert len(cols_undp) == 6

    # OPRI school life expectancy columns
    cols_opri = [
        "school_life_expectancy__pre_primary__both_sexes__years",
        "school_life_expectancy__pre_primary__female__years",
        "school_life_expectancy__pre_primary__male__years",
        "school_life_expectancy__primary__both_sexes__years",
        "school_life_expectancy__primary__female__years",
        "school_life_expectancy__primary__male__years",
        "school_life_expectancy__secondary__both_sexes__years",
        "school_life_expectancy__secondary__female__years",
        "school_life_expectancy__secondary__male__years",
        "school_life_expectancy__tertiary__both_sexes__years",
        "school_life_expectancy__tertiary__female__years",
        "school_life_expectancy__tertiary__male__years",
    ]

    # Gender statistics columns
    cols_gender_stats = [
        "hd_hci_lays",
        "hd_hci_lays_fe",
        "hd_hci_lays_ma",
    ]

    # Select only the relevant columns
    tb_undp = tb_undp.loc[:, ["country", "year"] + list(cols_undp)]
    tb_opri = tb_opri.loc[:, ["country", "year"] + list(cols_opri)]
    tb_gender_stats = tb_gender_stats.loc[:, ["country", "year"] + cols_gender_stats]

    #
    # Adjust dimensions
    #
    tb_undp = adjust_dimensions_schooling(tb_undp)
    tb_opri = adjust_dimensions_schooling(tb_opri)
    tb_gender_stats = adjust_dimensions_schooling(tb_gender_stats)

    #
    # Create collection object
    #
    collections = []
    for tb in [tb_undp, tb_opri, tb_gender_stats]:
        c_ = paths.create_collection(
            config=config,
            tb=tb,
            common_view_config=MULTIDIM_CONFIG,
        )
        collections.append(c_)

    c = combine_collections(
        collections=collections,
        collection_name=paths.short_name,
        config=config,
    )

    #
    # Group by gender and education level.
    #

    # Add grouped view
    c.group_views(
        groups=[
            {
                "dimension": "sex",
                "choice_new_slug": "sex_side_by_side",
                "choices": ["girls", "boys"],
                "view_config": {
                    "originUrl": "ourworldindata.org/education",
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "add-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "selectedFacetStrategy": "entity",
                },
            },
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "choices": {"preprimary", "primary", "secondary", "tertiary"},
                "view_config": {
                    "originUrl": "ourworldindata.org/education",
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "add-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["StackedArea"],
                    "selectedFacetStrategy": "entity",
                },
            },
            {
                "dimension": "metric_type",
                "choice_new_slug": "metric_type_side_by_side",
                "choices": ["expected_years_schooling", "average_years_schooling", "learning_adjusted_years_schooling"],
                "view_config": {
                    "originUrl": "ourworldindata.org/education",
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "add-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "selectedFacetStrategy": "entity",
                },
            },
        ],
    )

    # Drop views that are not relevant: For `level`="metric_type_side_by_side", there is only one valid `level` choice (`level`="all"). All the other level choices should be dropped.
    c.drop_views(
        {
            "metric_type": "metric_type_side_by_side",
            "level": [d for d in c.dimension_choices["level"] if d != "all"],
        }
    )

    for view in c.views:
        # Update title and subtitle based on view dimensions
        sex = view.dimensions["sex"]
        level = view.dimensions["level"]
        metric_type = view.dimensions["metric_type"]

        # Create a copy of the config to avoid shared references
        view.config = view.config.copy()
        # Add footnote for gross enrolment ratio
        if metric_type == "average_years_schooling":
            view.config["note"] = "Formal education is [primary](#dod:primary-education) or higher."

        # Generate dynamic title
        if sex and level and metric_type:
            view.config["title"] = generate_title_by_gender_level_and_metric(sex, level, metric_type)

        # Generate dynamic subtitle
        if level and metric_type:
            view.config["subtitle"] = generate_subtitle_by_level(level, metric_type, sex)

        if sex == "sex_side_by_side" or level == "level_side_by_side":
            view.metadata = {
                "description_from_producer": "",
                "description_short": view.config["subtitle"],
                "presentation": {
                    "title_public": view.config["title"],
                },
            }
        elif metric_type == "metric_type_side_by_side":
            view.metadata = {
                "description_from_producer": "",
                "description_short": view.config["subtitle"],
                "description_key": METRIC_TYPE_DESCRIPTION_KEY,
                "presentation": {
                    "title_public": view.config["title"],
                },
            }
        else:
            # Only updated description_short for other views
            view.metadata = {
                "description_short": view.config["subtitle"],
            }

        edit_indicator_displays(view)

    #
    # Save garden dataset.
    #
    c.save()


def adjust_dimensions_schooling(tb):
    """
    Add dimensions to schooling table columns.
    """

    # Helper maps for pattern matching
    level_keywords = {
        "pre_primary": "preprimary",
        "primary": "primary",
        "secondary": "secondary",
        "tertiary": "tertiary",
    }

    sex_keywords = {
        "both_sexes": "both",
        "total": "both",
        "male": "boys",
        "female": "girls",
        "ma": "boys",
        "fe": "girls",
    }

    metric_keywords = {
        "eys": "expected_years_schooling",
        "mys": "average_years_schooling",
        "school_life_expectancy": "expected_years_schooling",
        "hd_hci_lays": "learning_adjusted_years_schooling",
    }
    cols_to_add_dimensions = [col for col in tb.columns if col not in ["country", "year"]]

    # Iterate and populate mappings
    for col in cols_to_add_dimensions:
        # --- Level ---
        level = None
        for key, val in level_keywords.items():
            if key in col:
                level = val
                break

        # Default to "all" for aggregate measures
        if level is None and any(x in col for x in ["eys", "mys", "hd_hci_lays"]):
            level = "all"

        # --- Metric Type ---
        metric_type = None
        for key, val in metric_keywords.items():
            if key in col:
                metric_type = val
                break

        # --- Sex ---
        sex = None
        for key, val in sex_keywords.items():
            if f"__{key}__" in col or col.endswith(f"_{key}") or f"__{key}" in col:
                sex = val
                break

        # Set indicator name
        tb[col].metadata.original_short_name = "expected_years_schooling"
        # Set dimensions
        tb[col].metadata.dimensions = {
            "metric_type": metric_type,
            "level": level,
            "sex": sex or "both",  # fallback for non-disaggregated vars
        }

    # Add dimension definitions at table level
    if not hasattr(tb.metadata, "dimensions") or tb.metadata.dimensions is None:
        tb.metadata.dimensions = []

    # Only add if not already present
    existing_slugs = {dim.get("slug") for dim in tb.metadata.dimensions if isinstance(dim, dict)}
    new_dimensions = [
        {"name": "Metric", "slug": "metric_type"},
        {"name": "Education level", "slug": "level"},
        {"name": "Gender", "slug": "sex"},
    ]

    for dim in new_dimensions:
        if dim["slug"] not in existing_slugs:
            tb.metadata.dimensions.append(dim)

    return tb


def _get_gender_term(sex, level, context="title", metric_type=None):
    """Get appropriate gender term based on context, level, and metric type."""
    # Check for metric-specific mappings first
    if metric_type and metric_type in GENDER_MAPPINGS and sex in GENDER_MAPPINGS[metric_type]:
        return GENDER_MAPPINGS[metric_type][sex]
    # Check for tertiary-specific mappings
    elif level == "tertiary" and sex in GENDER_MAPPINGS["tertiary"]:
        return GENDER_MAPPINGS["tertiary"][sex]
    # Fall back to context-specific mappings
    return GENDER_MAPPINGS[context].get(sex, "")


def generate_title_by_gender_level_and_metric(sex, level, metric_type):
    """Generate title based on gender, education level, and metric type."""
    gender_term = _get_gender_term(sex, level, "title", metric_type)
    level_term = LEVEL_MAPPINGS["title"].get(level, "")

    if not level_term:
        raise ValueError(f"Unknown education level: {level}")

    # Handle grouped views first
    if metric_type == "metric_type_side_by_side":
        return f"Years of schooling among {gender_term}, by measure"
    elif level == "level_side_by_side":
        metric_term = METRIC_MAPPINGS.get(metric_type, "")
        if not metric_term:
            raise ValueError(f"Unknown metric type: {metric_type}")
        return f"{metric_term} among {gender_term} by education level"
    else:
        # Regular views - look up metric type
        metric_term = METRIC_MAPPINGS.get(metric_type, "")
        if not metric_term:
            raise ValueError(f"Unknown metric type: {metric_type}")

        if level == "all":
            return f"{metric_term} among {gender_term}"
        else:
            return f"{metric_term} among {gender_term} in {level_term}"


def generate_subtitle_by_level(level, metric_type, sex=None):
    # Handle metric type side-by-side case
    if metric_type == "metric_type_side_by_side":
        return "Average years show how many years of education today's adults completed, expected years indicate how long new students are likely to stay in school, and [learning-adjusted years of schooling](#dod:lays) predict how many effective years of schooling new students would receive once adjusted for how much they learn."

    # Helper functions for subject terms
    def get_subject(sex_key, is_adult=False):
        if is_adult:
            return {
                "boys": "men",
                "girls": "women",
                "sex_side_by_side": "men and women",
                "both": "adults",
                None: "adults",
            }.get(sex_key, "adults")
        else:
            return {
                "boys": "boys",
                "girls": "girls",
                "sex_side_by_side": "boys and girls",
                "both": "children",
                None: "children",
            }.get(sex_key, "children")

    def get_child_subject(sex_key):
        return {"boys": "a boy", "girls": "a girl", "both": "a child", None: "a child"}.get(sex_key, "a child")

    # Average years of schooling (adults aged 25+)
    if metric_type == "average_years_schooling":
        subject = get_subject(sex, is_adult=True)
        if sex == "sex_side_by_side":
            return "Average number of years adults aged 25 and older have spent in formal education, shown separately for men and women."
        else:
            return f"Average number of years {subject} aged 25 and older have spent in formal education."

    # Learning-adjusted years of schooling
    elif metric_type == "learning_adjusted_years_schooling":
        if sex == "sex_side_by_side":
            return "[Learning-adjusted years of schooling](#dod:lays) combine the quantity and quality of education into one metric, accounting for the fact that similar durations of schooling can yield different learning outcomes, shown separately for boys and girls."
        else:
            subject = get_subject(sex)
            return f"[Learning-adjusted years of schooling](#dod:lays) among {subject} combine the quantity and quality of education into one metric, accounting for the fact that similar durations of schooling can yield different learning outcomes."

    # Expected years of schooling
    elif metric_type == "expected_years_schooling":
        # Helper for consistent phrasing
        def expected_description(target, scope):
            return f"For each year, this shows how many years {target} starting school then can expect to spend {scope} if current enrollment patterns stay the same. It includes repeated grades."

        # Grouped by education level
        if level == "level_side_by_side":
            target = get_subject(sex)
            return expected_description(target, "at each education level")

        # All education levels
        elif level == "all":
            target = get_child_subject(sex) if sex in {"boys", "girls"} else "a child"
            scope = "in education across all levels, from [pre-primary](#dod:pre-primary-education) through [tertiary](#dod:tertiary-education)"
            return expected_description(target, scope)

        # Specific education level
        else:
            level_link = LEVEL_MAPPINGS["subtitle"].get(level, level)
            target = get_child_subject(sex) if sex in {"boys", "girls"} else "a child"
            scope = f"in {level_link} education"
            return expected_description(target, scope)


def edit_indicator_displays(view):
    """Edit display names and colors for the grouped views."""

    # Handle level side-by-side views (education levels)
    if view.dimensions["level"] == "level_side_by_side":
        # Display name and color mappings for education levels
        LEVEL_CONFIG = {
            "pre_primary": {"name": "Pre-primary", "color": COLOR_PREPRIMARY, "patterns": ["expectancy__pre_primary"]},
            "primary": {"name": "Primary", "color": COLOR_PRIMARY, "patterns": ["expectancy__primary"]},
            "secondary": {"name": "Secondary", "color": COLOR_SECONDARY, "patterns": ["secondary"]},
            "tertiary": {"name": "Tertiary", "color": COLOR_TERTIARY, "patterns": ["tertiary"]},
        }

        for indicator in view.indicators.y:
            for config in LEVEL_CONFIG.values():
                if any(pattern in indicator.catalogPath for pattern in config["patterns"]):
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

        # Sort indicators: pre-primary → primary → secondary → tertiary
        def get_level_index(ind):
            if "pre_primary" in ind.catalogPath:
                return 3
            elif "primary" in ind.catalogPath and "pre" not in ind.catalogPath:
                return 2
            elif "secondary" in ind.catalogPath:
                return 1
            elif "tertiary" in ind.catalogPath:
                return 0
            return 4

        view.indicators.y = sorted(view.indicators.y, key=get_level_index)

    # Handle sex side-by-side views (gender)
    elif view.dimensions["sex"] == "sex_side_by_side":
        # Display name and color mappings for gender
        GENDER_CONFIG = {
            "male": {"name": "Boys", "color": COLOR_BOYS, "patterns": ["__male__", "_ma"]},
            "female": {"name": "Girls", "color": COLOR_GIRLS, "patterns": ["__female__", "_fe"]},
        }

        for indicator in view.indicators.y:
            for config in GENDER_CONFIG.values():
                if any(
                    pattern in indicator.catalogPath or indicator.catalogPath.endswith(pattern)
                    for pattern in config["patterns"]
                ):
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

    # Handle metric type side-by-side views (different schooling measures)
    elif view.dimensions["metric_type"] == "metric_type_side_by_side":
        # Display name and color mappings for metric types
        METRIC_TYPE_CONFIG = {
            "expected": {
                "name": "Expected years",
                "color": COLOR_EXPECTED_YEARS,
                "patterns": ["eys", "school_life_expectancy"],
            },
            "average": {"name": "Average years", "color": COLOR_AVERAGE_YEARS, "patterns": ["mys"]},
            "learning_adjusted": {
                "name": "Learning-adjusted years",
                "color": COLOR_LEARNING_ADJUSTED,
                "patterns": ["hd_hci_lays"],
            },
        }

        for indicator in view.indicators.y:
            for config in METRIC_TYPE_CONFIG.values():
                if any(pattern in indicator.catalogPath for pattern in config["patterns"]):
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

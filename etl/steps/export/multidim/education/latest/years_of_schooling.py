"""Load a meadow dataset and create a garden dataset."""

from etl.collection import combine_collections
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "tab": "map",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "change-country",
}

# Common mappings for title and subtitle generation
GENDER_MAPPINGS = {
    "title": {"both": "children", "boys": "boys", "girls": "girls", "sex_side_by_side": "girls and boys"},
    "subtitle": {"both": "children", "boys": "boys", "girls": "girls", "sex_side_by_side": "boys and girls"},
    "tertiary": {"both": "young people", "boys": "men", "girls": "women", "sex_side_by_side": "men and women"},
}

LEVEL_MAPPINGS = {
    "title": {
        "primary": "primary education",
        "preprimary": "pre-primary education",
        "secondary": "secondary education",
        "tertiary": "tertiary education",
        "all": "all education levels",
        "level_side_by_side": "education",
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

METRIC_DESCRIPTION_MAP = {
    "expected_years_schooling": "Expected years of schooling is the number of years a person is expected to spend in school or university, including years spent on repetition.",
    "average_years_schooling": "Average years of schooling is the average number of completed years of education of a population.",
    "learning_adjusted_years_schooling": "Learning-adjusted years of schooling is the expected years of schooling adjusted for quality of learning.",
}


def _get_gender_term(sex, level, context="title"):
    """Get appropriate gender term based on context and level."""
    if level == "tertiary" and sex in GENDER_MAPPINGS["tertiary"]:
        return GENDER_MAPPINGS["tertiary"][sex]
    return GENDER_MAPPINGS[context].get(sex, "")


def generate_title_by_gender_level_and_metric(sex, level, metric_type):
    """Generate title based on gender, education level, and metric type."""
    gender_term = _get_gender_term(sex, level, "title")
    level_term = LEVEL_MAPPINGS["title"].get(level, "")
    metric_term = METRIC_MAPPINGS.get(metric_type, "")

    if not level_term:
        raise ValueError(f"Unknown education level: {level}")
    if not metric_term:
        raise ValueError(f"Unknown metric type: {metric_type}")

    if level == "level_side_by_side":
        return f"{metric_term} among {gender_term} by education level"
    elif level == "all":
        return f"{metric_term} among {gender_term}"
    else:
        return f"{metric_term} among {gender_term} in {level_term}"


def generate_subtitle_by_level(level, sex, metric_type):
    """Generate subtitle based on education level, gender, and metric type with links."""
    level_term = LEVEL_MAPPINGS["subtitle"].get(level, "")
    gender_term = _get_gender_term(sex, level, "subtitle")
    metric_description = METRIC_DESCRIPTION_MAP.get(metric_type, "")

    if not level_term:
        raise ValueError(f"Unknown education level: {level}")
    if not metric_description:
        raise ValueError(f"Unknown metric type: {metric_type}")

    if level_term and gender_term:
        return f"{metric_description} This is shown for {gender_term} in {level_term} education."
    elif level_term:
        return f"{metric_description} This is shown for {level_term} education."
    else:
        return metric_description


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
    CHOICES_EDUCATION = c.get_choice_names("level")
    CHOICES_METRIC_TYPE = c.get_choice_names("metric_type")
    CHOICES_SEX = c.get_choice_names("sex")

    METRIC_SUBTITLES_ALL_GENDERS = {
        "Expected years of schooling": "Expected years of schooling is the number of years a person is expected to spend in school or university, including years spent on repetition.",
        "Average years of schooling": "Average years of schooling is the average number of completed years of education of a population.",
        "Learning adjusted years of schooling": "Learning-adjusted years of schooling is the expected years of schooling adjusted for quality of learning.",
    }

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
                    "addCountryMode": "change-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "selectedFacetStrategy": "entity",
                    "title": "{metric} for {level} education among boys and girls",
                    "subtitle": "{subtitle_all_genders}",
                },
            },
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "choices": {"preprimary", "primary", "secondary", "tertiary"},
                "view_config": {
                    "originUrl": "ourworldindata.org/education",
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "change-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["StackedArea"],
                    "selectedFacetStrategy": "entity",
                    "title": "{metric} among {sex} for all education levels",
                },
            },
        ],
        params={
            "level": lambda view: (
                CHOICES_EDUCATION.get(view.dimensions.get("level"), "").lower() if view.dimensions.get("level") else ""
            ),
            "metric": lambda view: CHOICES_METRIC_TYPE.get(view.dimensions.get("metric_type"), ""),
            "subtitle_all_genders": lambda view: METRIC_SUBTITLES_ALL_GENDERS.get(
                CHOICES_METRIC_TYPE.get(view.dimensions.get("metric_type"))
            ),
            "sex": lambda view: (
                CHOICES_SEX.get(view.dimensions.get("sex"), "").lower() if view.dimensions.get("sex") else ""
            ),
        },
    )

    for view in c.views:
        # Update title and subtitle based on view dimensions
        sex = view.dimensions["sex"]
        level = view.dimensions["level"]
        metric_type = view.dimensions["metric_type"]

        # Create a copy of the config to avoid shared references
        view.config = view.config.copy()

        # Generate dynamic title
        if sex and level and metric_type:
            view.config["title"] = generate_title_by_gender_level_and_metric(sex, level, metric_type)

        # Generate dynamic subtitle
        if level and metric_type:
            view.config["subtitle"] = generate_subtitle_by_level(level, sex, metric_type)

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


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.dimensions["level"] == "level_side_by_side":
        assert view.indicators.y is not None
        for indicator in view.indicators.y:
            display_name = None
            if "expectancy__primary" in indicator.catalogPath:
                display_name = "Primary"
            elif "expectancy__pre_primary" in indicator.catalogPath:
                display_name = "Pre-primary"
            elif "secondary" in indicator.catalogPath:
                display_name = "Secondary"
            elif "tertiary" in indicator.catalogPath:
                display_name = "Tertiary"
            elif "all" in indicator.catalogPath or "eys" in indicator.catalogPath or "mys" in indicator.catalogPath:
                display_name = "All levels"

            if display_name:
                indicator.display = {
                    "name": display_name,
                }

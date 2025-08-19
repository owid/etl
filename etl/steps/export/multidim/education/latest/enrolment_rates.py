"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "hasMapTab": True,
    "tab": "map",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "add-country",
    "yAxis": {"min": 0, "max": 100},
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds_sdgs = paths.load_dataset("education_sdgs")
    tb_sdgs = ds_sdgs.read("education_sdgs", load_data=False)
    ds_opri = paths.load_dataset("education_opri")
    tb_opri = ds_opri.read("education_opri", load_data=False)

    ds_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_wdi.read("wdi", load_data=False)

    cols_opri = [
        "total_net_enrolment_rate__primary__both_sexes__pct",
        "total_net_enrolment_rate__primary__male__pct",
        "total_net_enrolment_rate__primary__female__pct",
        "total_net_enrolment_rate__lower_secondary__both_sexes__pct",
        "total_net_enrolment_rate__lower_secondary__male__pct",
        "total_net_enrolment_rate__lower_secondary__female__pct",
        "total_net_enrolment_rate__upper_secondary__both_sexes__pct",
        "total_net_enrolment_rate__upper_secondary__male__pct",
        "total_net_enrolment_rate__upper_secondary__female__pct",
        "gross_enrolment_ratio__primary__both_sexes__pct",
        "gross_enrolment_ratio__primary__male__pct",
        "gross_enrolment_ratio__primary__female__pct",
        "gross_enrolment_ratio__lower_secondary__both_sexes__pct",
        "gross_enrolment_ratio__lower_secondary__male__pct",
        "gross_enrolment_ratio__lower_secondary__female__pct",
        "gross_enrolment_ratio__upper_secondary__both_sexes__pct",
        "gross_enrolment_ratio__upper_secondary__male__pct",
        "gross_enrolment_ratio__upper_secondary__female__pct",
    ]

    cols_sdgs = [
        "gross_enrolment_ratio_for_tertiary_education__both_sexes__pct__ger_5t8",
        "gross_enrolment_ratio_for_tertiary_education__female__pct__ger_5t8_f",
        "gross_enrolment_ratio_for_tertiary_education__male__pct__ger_5t8_m",
        "net_enrolment_rate__pre_primary__both_sexes__pct__ner_02_cp",
        "net_enrolment_rate__pre_primary__female__pct__ner_02_f_cp",
        "net_enrolment_rate__pre_primary__male__pct__ner_02_m_cp",
    ]

    cols_wdi = [
        "se_pre_enrr",
        "se_pre_enrr_ma",
        "se_pre_enrr_fe",
    ]

    # Select only the relevant columns from tb_sdgs
    tb_opri = tb_opri[["country", "year"] + cols_opri]
    tb_sdgs = tb_sdgs[["country", "year"] + cols_sdgs]
    tb_wdi = tb_wdi[["country", "year"] + cols_wdi]
    #
    # Adjust dimensions
    #
    tb_sdgs = adjust_dimensions_enrolment(tb_sdgs)
    tb_opri = adjust_dimensions_enrolment(tb_opri)
    tb_wdi = adjust_dimensions_enrolment(tb_wdi)
    #
    # Create collection object
    #

    c = paths.create_collection(
        config=config,
        tb=[tb_sdgs, tb_opri, tb_wdi],
        common_view_config=MULTIDIM_CONFIG,
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
                    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
                    "originUrl": "ourworldindata.org/education",
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "add-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "selectedFacetStrategy": "entity",
                    "yAxis": {"min": 0, "max": 100},
                },
            },
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "view_config": {
                    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
                    "originUrl": "ourworldindata.org/education",
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "add-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "selectedFacetStrategy": "entity",
                    "yAxis": {"min": 0, "max": 100},
                },
            },
        ]
    )

    for view in c.views:
        # Update title and subtitle based on view dimensions
        sex = view.dimensions["sex"]
        level = view.dimensions["level"]
        enrolment_type = view.dimensions["enrolment_type"]

        # Create a copy of the config to avoid shared references
        view.config = view.config.copy()

        # Generate dynamic title
        if sex and level:
            view.config["title"] = generate_title_by_gender_and_level(sex, level)

        # Generate dynamic subtitle
        if level and enrolment_type:
            view.config["subtitle"] = generate_subtitle_by_level(level, sex, enrolment_type)
        
        # Add footnote for gross enrolment ratio
        if enrolment_type == "gross_enrolment":
            view.config["note"] = "Values may exceed 100% when children who are older or younger than the official age group also enroll."
        
        if sex == "sex_side_by_side" or level == "level_side_by_side":
            view.metadata = {
                "presentation": {
                    "title_public": view.config["title"],
                },
                "description_short": view.config["subtitle"],
            }

        edit_indicator_displays(view)

    #
    # Save garden dataset.
    #
    c.save()


def adjust_dimensions_enrolment(tb):
    """
    Add dimensions to enrolment table columns.

    """

    # Helper maps for pattern matching
    level_keywords = {
        "pre_primary": "preprimary",
        "pre_enrr": "preprimary",
        "primary": "primary",
        "lower_secondary": "lower_secondary",
        "upper_secondary": "upper_secondary",
        "tertiary": "tertiary",
    }

    sex_keywords = {"both_sexes": "both", "male": "boys", "female": "girls", "ma": "boys", "fe": "girls"}

    cols_to_add_dimensions = [col for col in tb.columns if col not in ["country", "year"]]

    # Iterate and set dimensions directly
    for col in cols_to_add_dimensions:
        tb[col].metadata.original_short_name = "enrolment_rates"
        tb[col].metadata.dimensions = {}

        # --- Level ---
        level = None
        for key, val in level_keywords.items():
            if key in col:
                level = val
                break

        # --- Enrolment Type ---
        if "gross" in col:
            enrolment_type = "gross_enrolment"
        elif "net" in col:
            enrolment_type = "net_enrolment"
        elif "pre_enrr" in col:
            enrolment_type = "gross_enrolment"
        else:
            raise ValueError(f"Unrecognized enrolment type for column {col}")

        # --- Sex ---
        sex = None
        for key, val in sex_keywords.items():
            if f"__{key}__" in col or col.endswith(f"_{key}"):
                sex = val
                break

        # Set dimensions
        tb[col].metadata.dimensions["enrolment_type"] = enrolment_type
        tb[col].metadata.dimensions["level"] = level
        tb[col].metadata.dimensions["sex"] = sex or "both"

    # Add dimension definitions at table level
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.extend(
            [
                {"name": "Metric", "slug": "enrolment_type"},
                {"name": "Education level", "slug": "level"},
                {"name": "Gender", "slug": "sex"},
            ]
        )

    return tb


# Common mappings used by both title and subtitle functions
GENDER_MAPPINGS = {
    "title": {"both": "children", "boys": "boys", "girls": "girls", "sex_side_by_side": "children"},
    "subtitle": {"both": "children", "boys": "boys", "girls": "girls", "sex_side_by_side": "boys and girls"},
    "tertiary": {"both": "people", "boys": "men", "girls": "women", "sex_side_by_side": "people"},
}

LEVEL_MAPPINGS = {
    "title": {
        "primary": "primary school",
        "preprimary": "pre-primary school",
        "lower_secondary": "lower secondary school",
        "upper_secondary": "upper secondary school",
        "tertiary": "tertiary education",
        "level_side_by_side": "school",
    },
    "subtitle": {
        "primary": "[primary](#dod:primary-education)",
        "preprimary": "[pre-primary](#dod:pre-primary-education)",
        "lower_secondary": "[lower secondary](#dod:lower-secondary-education)",
        "upper_secondary": "[upper secondary](#dod:upper-secondary-education)",
        "tertiary": "[tertiary](#dod:tertiary-education)",
        "level_side_by_side": "[pre-primary](#dod:pre-primary-education), [primary](#dod:primary-education), [lower secondary](#dod:lower-secondary-education), [upper secondary](#dod:upper-secondary-education), and [tertiary](#dod:tertiary-education)",
    },
}

ENROLMENT_TYPE_MAP = {
    "net_enrolment": "This is shown as the [net enrolment ratio](#dod:net-enrolment-ratio)",
    "gross_enrolment": "This is shown as the [gross enrolment ratio](#dod:gross-enrolment-ratio)",
}


def _get_gender_term(sex, level, context="title"):
    """Get appropriate gender term based on context and level."""
    if level == "tertiary" and sex in GENDER_MAPPINGS["tertiary"]:
        return GENDER_MAPPINGS["tertiary"][sex]
    return GENDER_MAPPINGS[context].get(sex, "")


def generate_title_by_gender_and_level(sex, level):
    """Generate title based on gender and education level."""
    gender_term = _get_gender_term(sex, level, "title")
    level_term = LEVEL_MAPPINGS["title"].get(level, "")

    if not level_term:
        raise ValueError(f"Unknown education level: {level}")

    if level == "level_side_by_side":
        return f"Share of {gender_term} enrolled in school, by education level"

    return f"Share of {gender_term} enrolled in {level_term}"


def generate_subtitle_by_level(level, sex, enrolment_type):
    """Generate subtitle based on education level, gender, and enrollment type with links."""
    level_term = LEVEL_MAPPINGS["subtitle"].get(level, "")
    gender_term = _get_gender_term(sex, level, "subtitle")
    enrolment_description = ENROLMENT_TYPE_MAP.get(enrolment_type, "")

    if not level_term:
        raise ValueError(f"Unknown education level: {level}")
    if not enrolment_description:
        raise ValueError(f"Unknown enrolment type: {enrolment_type}")

    if level_term and gender_term:
        return f"{enrolment_description} for {gender_term} in {level_term} education."
    elif level_term:
        return f"{enrolment_description} for {level_term} education."


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.dimensions["level"] == "level_side_by_side":
        assert view.indicators.y is not None
        for indicator in view.indicators.y:
            display_name = None

            if (
                "enrolment_rate__primary" in indicator.catalogPath
                or "enrolment_ratio__primary" in indicator.catalogPath
            ):
                display_name = "Primary"
            elif "enrolment_rate__pre_primary" in indicator.catalogPath or "pre_enrr" in indicator.catalogPath:
                display_name = "Pre-primary"
            elif "lower_secondary" in indicator.catalogPath:
                display_name = "Lower secondary"
            elif "upper_secondary" in indicator.catalogPath:
                display_name = "Upper secondary"
            elif "tertiary" in indicator.catalogPath:
                display_name = "Tertiary"

            if display_name:
                indicator.display = {
                    "name": display_name,
                }

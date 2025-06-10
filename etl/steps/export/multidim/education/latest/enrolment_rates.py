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

    collections = []
    for tb in [tb_sdgs, tb_opri, tb_wdi]:
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
    for view in c.views:
        # Update title and subtitle based on view dimensions
        sex = view.dimensions.get("sex")
        level = view.dimensions.get("level")
        enrolment_type = view.dimensions.get("enrolment_type")

        # Generate dynamic title
        if sex and level:
            view.config["title"] = generate_title_by_gender_and_level(sex, level)

        # Generate dynamic subtitle
        if level and enrolment_type:
            view.config["subtitle"] = generate_subtitle_by_level(level, enrolment_type)

    #
    # Group by gender and education level.
    #
    CHOICES_EDUCATION = c.get_choice_names("level")
    CHOICES_ENROLMENT_TYPE = c.get_choice_names("enrolment_type")
    CHOICES_SEX = c.get_choice_names("sex")
    METRIC_SUBTITLES_ALL_GENDERS = {
        "Net enrolment": "This is shown as the [net enrolment rates](#dod:net-enrolment-ratio).",
        "Gross enrolment": "This is shown as the [gross enrolment rates](#dod:gross-enrolment-ratio).",
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
                    "title": "Share of children who are enroled in {level} education, by gender",
                    "subtitle": "{subtitle_all_genders}",
                },
            },
            {
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "view_config": {
                    "originUrl": "ourworldindata.org/education",
                    "hideAnnotationFieldsInTitle": {"time": True},
                    "addCountryMode": "change-country",
                    "hasMapTab": False,
                    "tab": "chart",
                    "selectedFacetStrategy": "entity",
                    "title": "Share of {sex} who are enroled school, by education level",
                },
            },
        ],
        params={
            "level": lambda view: (
                CHOICES_EDUCATION.get(view.dimensions.get("level"), "").lower() if view.dimensions.get("level") else ""
            ),
            "metric": lambda view: CHOICES_ENROLMENT_TYPE.get(view.dimensions.get("enrolment_type"), ""),
            "subtitle_all_genders": lambda view: METRIC_SUBTITLES_ALL_GENDERS.get(
                CHOICES_ENROLMENT_TYPE.get(view.dimensions.get("enrolment_type"))
            ),
            "sex": lambda view: (
                CHOICES_SEX.get(view.dimensions.get("sex"), "").lower() if view.dimensions.get("sex") else ""
            ),
        },
    )
    for view in c.views:
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


def generate_title_by_gender_and_level(sex, level):
    """Generate title based on gender and education level."""
    # Map gender to appropriate term
    gender_map = {"both": "children", "boys": "boys", "girls": "girls"}

    # Map level to appropriate term (for titles - simple text)
    level_map = {
        "primary": "primary",
        "preprimary": "pre-primary",
        "lower_secondary": "lower secondary",
        "upper_secondary": "upper secondary",
        "tertiary": "tertiary",
    }

    gender_term = gender_map.get(sex, "children")
    level_term = level_map.get(level, "")

    return f"The share of {gender_term} who are enrolled in {level_term} school"


def generate_subtitle_by_level(level, enrolment_type):
    """Generate subtitle based on education level and enrollment type with links."""
    # Map level to appropriate term with links (for subtitles)
    level_map = {
        "primary": "[primary](#dod:primary-education)",
        "preprimary": "[pre-primary](#dod:pre-primary-education)",
        "lower_secondary": "[lower secondary](#dod:lower-secondary-education)",
        "upper_secondary": "[upper secondary](#dod:upper-secondary-education)",
        "tertiary": "[tertiary](#dod:tertiary-education)",
    }

    # Map enrollment type to appropriate description with links
    enrolment_type_map = {
        "net_enrolment": "[net enrolment rates](#dod:net-enrolment-ratio)",
        "gross_enrolment": "[gross enrolment rates](#dod:gross-enrolment-ratio)",
    }

    level_term = level_map.get(level, "")
    enrolment_description = enrolment_type_map.get(enrolment_type, "This is shown as the")

    return f"{enrolment_description} for {level_term} education."


def edit_indicator_displays(view):
    """Edit display names for the grouped views."""
    if view.dimensions["level"] == "level_side_by_side":
        assert view.indicators.y is not None
        for indicator in view.indicators.y:
            display_name = "Unknown"  # Default value
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

            indicator.display = {
                "name": display_name,
            }

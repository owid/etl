"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Color constants for education levels and gender
COLOR_PREPRIMARY = "#D73C50"
COLOR_PRIMARY = "#4C6A9C"
COLOR_LOWER_SECONDARY = "#883039"
COLOR_UPPER_SECONDARY = "#578145"
COLOR_TERTIARY = "#B16214"

COLOR_BOYS = "#00847E"
COLOR_GIRLS = "#E56E5A"

# Color constants for enrolment types
COLOR_NET_ENROLMENT = "#883039"
COLOR_GROSS_ENROLMENT = "#4C6A9C"

# Used in enrolment type side-by-side comparison views
ENROLLMENT_TYPE_DESCRIPTION_KEY = [
    "Net and gross enrolment ratios measure school participation from different perspectives, providing complementary insights into education systems.",
    "**Net enrolment** shows what percentage of children are enrolled at the education level intended for their age. It compares children enrolled at the correct level to the total population in that age group. The maximum value is 100%.",
    "**Gross enrolment** counts all students enrolled at a specific education level, regardless of age. It includes students who started early, late, or repeated grades. Values can exceed 100% when older or younger students are enrolled.",
    "High net enrolment indicates children are progressing at the expected pace. High gross enrolment above 100% may signal grade repetition or late school entry.",
    "Low net enrolment suggests children are out of school or enrolled at different levels than expected for their age. Low gross enrolment indicates many children of the official age are not enrolled at the expected level.",
    "Data comes from school administrative records tracking enrolment by age, combined with population estimates from national statistics offices or UN sources.",
]

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
            {
                "dimension": "enrolment_type",
                "choice_new_slug": "enrolment_type_side_by_side",
                "choices": ["net_enrolment", "gross_enrolment"],
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
            if enrolment_type == "enrolment_type_side_by_side":
                # For enrolment type grouping, get the actual level from the view's indicators
                actual_level = None
                for indicator in view.indicators.y:
                    if hasattr(indicator, "metadata") and hasattr(indicator.metadata, "dimensions"):
                        if "level" in indicator.metadata.dimensions:
                            actual_level = indicator.metadata.dimensions["level"]
                            break

                if actual_level:
                    gender_term = _get_gender_term(sex, actual_level, "title")
                    level_name = LEVEL_MAPPINGS["title"].get(actual_level, actual_level)
                    view.config["title"] = f"Share of {gender_term} enrolled in {level_name}, by enrolment measure"
                else:
                    view.config["title"] = generate_title_by_gender_and_level(sex, level)
            else:
                view.config["title"] = generate_title_by_gender_and_level(sex, level)

        # Generate dynamic subtitle
        if level and enrolment_type:
            view.config["subtitle"] = generate_subtitle_by_level(level, sex, enrolment_type)

        # Add footnote for gross enrolment ratio
        if enrolment_type == "gross_enrolment":
            view.config["note"] = (
                "Values may exceed 100% when children who are older or younger than the official age group also enroll."
            )

        if sex == "sex_side_by_side" or level == "level_side_by_side":
            view.metadata = {
                "description_from_producer": "",
                "description_short": view.config["subtitle"],
                "presentation": {
                    "title_public": view.config["title"],
                },
            }
        elif enrolment_type == "enrolment_type_side_by_side":
            view.metadata = {
                "description_from_producer": "",
                "description_short": view.config["subtitle"],
                "description_key": ENROLLMENT_TYPE_DESCRIPTION_KEY,
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
    # Drop views that are not relevant: For `level`="metric_type_side_by_side", there is only one valid `level` choice (`level`="all"). All the other level choices should be dropped.
    c.drop_views(
        {
            "metric_type": "enrolment_type_side_by_side",
            "level": [d for d in c.dimension_choices["level"] if d == "tertiary"],
        }
    )
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
    "title": {
        "both": "children",
        "boys": "boys",
        "girls": "girls",
        "sex_side_by_side": "children",
        "enrolment_type_side_by_side": "children",
    },
    "subtitle": {
        "both": "children",
        "boys": "boys",
        "girls": "girls",
        "sex_side_by_side": "boys and girls",
        "enrolment_type_side_by_side": "children",
    },
    "tertiary": {
        "both": "people",
        "boys": "men",
        "girls": "women",
        "sex_side_by_side": "people",
        "enrolment_type_side_by_side": "people",
    },
}

LEVEL_MAPPINGS = {
    "title": {
        "primary": "primary school",
        "preprimary": "pre-primary school",
        "lower_secondary": "lower secondary school",
        "upper_secondary": "upper secondary school",
        "tertiary": "tertiary education",
        "level_side_by_side": "school",
        "enrolment_type_side_by_side": "{level_name}",
    },
    "subtitle": {
        "primary": "[primary](#dod:primary-education)",
        "preprimary": "[pre-primary](#dod:pre-primary-education)",
        "lower_secondary": "[lower secondary](#dod:lower-secondary-education)",
        "upper_secondary": "[upper secondary](#dod:upper-secondary-education)",
        "tertiary": "[tertiary](#dod:tertiary-education)",
        "level_side_by_side": "[pre-primary](#dod:pre-primary-education), [primary](#dod:primary-education), [lower secondary](#dod:lower-secondary-education), [upper secondary](#dod:upper-secondary-education), and [tertiary](#dod:tertiary-education)",
        "level_side_by_side_gross": "[pre-primary](#dod:pre-primary-education), [primary](#dod:primary-education), [lower secondary](#dod:lower-secondary-education), [upper secondary](#dod:upper-secondary-education), and [tertiary](#dod:tertiary-education)",
        "level_side_by_side_net": "[pre-primary](#dod:pre-primary-education), [primary](#dod:primary-education), [lower secondary](#dod:lower-secondary-education), and [upper secondary](#dod:upper-secondary-education)",
        "enrolment_type_side_by_side": "{level_dod}",
    },
    "plain": {
        "primary": "primary school age (between 5 and 7 years old)",
        "preprimary": "pre-primary school age (between 3 and 5 years old)",
        "lower_secondary": "lower secondary school age (between 11 and 14 years old)",
        "upper_secondary": "upper secondary school age (between 15 and 18 years old)",
        "tertiary": "tertiary education age (typically between 18 and 22 years old)",
        "level_side_by_side_gross": "pre-primary, primary, lower secondary, upper secondary, and tertiary",
        "level_side_by_side_net": "pre-primary, primary, lower secondary, and upper secondary",
        "enrolment_type_side_by_side": "{level_plain}",
    },
}

NET_TEMPLATE = (
    "Shown as the [net enrolment ratio](#dod:net-enrolment-ratio) — "
    "the share of {subject} of {level_plain} who are enrolled in {level_dod} education."
)

NET_GROUPED_TEMPLATE = (
    "Shown for each level of education as the [net enrolment ratio](#dod:net-enrolment-ratio) — "
    "the share of {subject} within the official school-age group who are enrolled in the relevant level of education."
)

GROSS_TEMPLATE = (
    "Shown as the [gross enrolment ratio](#dod:gross-enrolment-ratio) — "
    "the number of {subject}, regardless of their age, who are enrolled in {level_dod} education, "
    "expressed as a percentage of {population} of {level_plain}."
)

GROSS_GROUPED_TEMPLATE = (
    "Shown for each level of education as the [gross enrolment ratio](#dod:gross-enrolment-ratio) — "
    "the number of {subject}, regardless of their age, who are enrolled at that level of education, "
    "expressed as a percentage of the official school-age population {population_suffix}."
)

ENROLMENT_TYPE_TEMPLATE = (
    "Comparing [net](#dod:net-enrolment-ratio) and [gross enrolment ratios](#dod:gross-enrolment-ratio) "
    "for {subject} in {level_dod} education. Net enrolment shows the share of {subject} of {level_plain} "
    "who are enrolled, while gross enrolment includes all {subject} regardless of age, expressed as a "
    "percentage of {population} of the same age group."
)

ENROLMENT_TYPE_MAP = {
    "net_enrolment": {
        "both": NET_TEMPLATE.format(subject="children", level_dod="{level_dod}", level_plain="{level_plain}"),
        "boys": NET_TEMPLATE.format(subject="boys", level_dod="{level_dod}", level_plain="{level_plain}"),
        "girls": NET_TEMPLATE.format(
            subject="girls",
            level_dod="{level_dod}",
            level_plain="{level_plain}",
        ),
        "tertiary_both": NET_TEMPLATE.format(subject="people", level_dod="{level_dod}", level_plain="{level_plain}"),
        "tertiary_men": NET_TEMPLATE.format(subject="men", level_dod="{level_dod}", level_plain="{level_plain}"),
        "tertiary_women": NET_TEMPLATE.format(
            subject="women",
            level_dod="{level_dod}",
            level_plain="{level_plain}",
        ),
        "both_grouped": NET_GROUPED_TEMPLATE.format(subject="children", population_suffix="of children"),
        "boys_grouped": NET_GROUPED_TEMPLATE.format(subject="boys", population_suffix="of boys"),
        "girls_grouped": NET_GROUPED_TEMPLATE.format(subject="girls", population_suffix="of girls"),
    },
    "gross_enrolment": {
        "both": GROSS_TEMPLATE.format(
            suffix="",
            subject="children",
            level_dod="{level_dod}",
            level_plain="{level_plain}",
            population="the population",
        ),
        "boys": GROSS_TEMPLATE.format(
            suffix=" for boys", subject="boys", level_dod="{level_dod}", level_plain="{level_plain}", population="boys"
        ),
        "girls": GROSS_TEMPLATE.format(
            suffix=" for girls",
            subject="girls",
            level_dod="{level_dod}",
            level_plain="{level_plain}",
            population="girls",
        ),
        "tertiary_both": GROSS_TEMPLATE.format(
            suffix="",
            subject="people",
            level_dod="{level_dod}",
            level_plain="{level_plain}",
            population="the population",
        ),
        "tertiary_men": GROSS_TEMPLATE.format(
            suffix=" for men", subject="men", level_dod="{level_dod}", level_plain="{level_plain}", population="men"
        ),
        "tertiary_women": GROSS_TEMPLATE.format(
            suffix=" for women",
            subject="women",
            level_dod="{level_dod}",
            level_plain="{level_plain}",
            population="women",
        ),
        "both_grouped": GROSS_GROUPED_TEMPLATE.format(suffix="", subject="children", population_suffix="of children"),
        "boys_grouped": GROSS_GROUPED_TEMPLATE.format(suffix=" for boys", subject="boys", population_suffix="of boys"),
        "girls_grouped": GROSS_GROUPED_TEMPLATE.format(
            suffix=" for girls", subject="girls", population_suffix="of girls"
        ),
    },
    "enrolment_type_side_by_side": {
        "both": ENROLMENT_TYPE_TEMPLATE.format(
            subject="children", level_dod="{level_dod}", level_plain="{level_plain}", population="the population"
        ),
        "boys": ENROLMENT_TYPE_TEMPLATE.format(
            subject="boys", level_dod="{level_dod}", level_plain="{level_plain}", population="boys"
        ),
        "girls": ENROLMENT_TYPE_TEMPLATE.format(
            subject="girls", level_dod="{level_dod}", level_plain="{level_plain}", population="girls"
        ),
        "tertiary_both": ENROLMENT_TYPE_TEMPLATE.format(
            subject="people", level_dod="{level_dod}", level_plain="{level_plain}", population="the population"
        ),
        "tertiary_men": ENROLMENT_TYPE_TEMPLATE.format(
            subject="men", level_dod="{level_dod}", level_plain="{level_plain}", population="men"
        ),
        "tertiary_women": ENROLMENT_TYPE_TEMPLATE.format(
            subject="women", level_dod="{level_dod}", level_plain="{level_plain}", population="women"
        ),
    },
}


def _get_gender_term(sex, level, context="title"):
    """Get appropriate gender term based on context and level."""
    if level == "tertiary" and sex in GENDER_MAPPINGS["tertiary"]:
        return GENDER_MAPPINGS["tertiary"][sex]
    return GENDER_MAPPINGS[context].get(sex, "")


def generate_title_by_gender_and_level(sex, level):
    """Generate title based on gender and education level."""
    gender_term = _get_gender_term(sex, level, "title")

    if level == "level_side_by_side":
        return f"Share of {gender_term} enrolled in school, by education level"
    elif level == "enrolment_type_side_by_side":
        # For enrolment type grouping, we need to determine the actual level
        # This will be handled dynamically in the main loop where we have access to the view
        return "Share of {gender_term} enrolled in {level_name}, by enrolment measure"

    level_term = LEVEL_MAPPINGS["title"].get(level, "")
    if not level_term:
        raise ValueError(f"Unknown education level: {level}")

    return f"Share of {gender_term} enrolled in {level_term}"


def generate_subtitle_by_level(level, sex, enrolment_type):
    """Generate subtitle based on education level, gender, and enrolment type with links."""

    # Handle enrolment type side-by-side case
    if enrolment_type == "enrolment_type_side_by_side":
        # Get the appropriate description key based on gender and level
        if level == "tertiary":
            if sex == "boys":
                gender_key = "tertiary_men"
            elif sex == "girls":
                gender_key = "tertiary_women"
            else:
                gender_key = "tertiary_both"
        else:
            if sex == "boys":
                gender_key = "boys"
            elif sex == "girls":
                gender_key = "girls"
            else:
                gender_key = "both"

        description_template = ENROLMENT_TYPE_MAP["enrolment_type_side_by_side"][gender_key]
        level_term_linked = LEVEL_MAPPINGS["subtitle"].get(level, "")
        level_term_plain = LEVEL_MAPPINGS["plain"].get(level, "")
        return description_template.format(level_dod=level_term_linked, level_plain=level_term_plain)

    # Get the appropriate description key based on level and gender
    if level == "level_side_by_side":
        # Use simplified grouped templates for level comparisons
        if sex == "boys":
            gender_key = "boys_grouped"
        elif sex == "girls":
            gender_key = "girls_grouped"
        else:
            gender_key = "both_grouped"
    elif level == "tertiary":
        if sex == "boys":
            gender_key = "tertiary_men"
        elif sex == "girls":
            gender_key = "tertiary_women"
        else:
            gender_key = "tertiary_both"
    else:
        if sex == "boys":
            gender_key = "boys"
        elif sex == "girls":
            gender_key = "girls"
        else:
            gender_key = "both"

    # Get the appropriate description template
    if enrolment_type not in ENROLMENT_TYPE_MAP:
        raise ValueError(f"Unknown enrolment type: {enrolment_type}")

    if gender_key not in ENROLMENT_TYPE_MAP[enrolment_type]:
        raise ValueError(f"Unknown gender key: {gender_key}")

    description_template = ENROLMENT_TYPE_MAP[enrolment_type][gender_key]

    # Format the template with both DoD-linked and plain level terms
    # For level_side_by_side, use specific keys based on enrolment type
    if level == "level_side_by_side":
        if enrolment_type == "gross_enrolment":
            level_key = "level_side_by_side_gross"
        else:
            level_key = "level_side_by_side_net"
    else:
        level_key = level

    level_term_linked = LEVEL_MAPPINGS["subtitle"].get(level_key, "")
    level_term_plain = LEVEL_MAPPINGS["plain"].get(level_key, "")

    return description_template.format(level_dod=level_term_linked, level_plain=level_term_plain)


def edit_indicator_displays(view):
    """Edit display names and colors for the grouped views."""

    # Handle level side-by-side views (education levels)
    if view.matches(level="level_side_by_side"):
        # Display name and color mappings for education levels
        LEVEL_CONFIG = {
            "primary": {
                "name": "Primary",
                "color": COLOR_PRIMARY,
                "patterns": ["enrolment_rate__primary", "enrolment_ratio__primary"],
            },
            "pre_primary": {
                "name": "Pre-primary",
                "color": COLOR_PREPRIMARY,
                "patterns": ["enrolment_rate__pre_primary", "pre_enrr"],
            },
            "lower_secondary": {
                "name": "Lower secondary",
                "color": COLOR_LOWER_SECONDARY,
                "patterns": ["lower_secondary"],
            },
            "upper_secondary": {
                "name": "Upper secondary",
                "color": COLOR_UPPER_SECONDARY,
                "patterns": ["upper_secondary"],
            },
            "tertiary": {"name": "Tertiary", "color": COLOR_TERTIARY, "patterns": ["tertiary"]},
        }

        for indicator in view.indicators.y:
            for config in LEVEL_CONFIG.values():
                if any(pattern in indicator.catalogPath for pattern in config["patterns"]):
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

    # Handle sex side-by-side views (gender)
    if view.matches(sex="sex_side_by_side"):
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

    # Handle enrolment type side-by-side views (net vs gross)
    if view.matches(enrolment_type="enrolment_type_side_by_side"):
        # Display name and color mappings for enrolment types
        ENROLMENT_TYPE_CONFIG = {
            "net": {"name": "Net enrolment", "color": COLOR_NET_ENROLMENT, "patterns": ["net_enrolment"]},
            "gross": {
                "name": "Gross enrolment",
                "color": COLOR_GROSS_ENROLMENT,
                "patterns": ["gross_enrolment", "pre_enrr"],
            },
        }

        for indicator in view.indicators.y:
            for config in ENROLMENT_TYPE_CONFIG.values():
                if any(pattern in indicator.catalogPath for pattern in config["patterns"]):
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

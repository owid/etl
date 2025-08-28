"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Color constants
COLOR_PREPRIMARY = "#D73C50"
COLOR_PRIMARY = "#4C6A9C"
COLOR_LOWER_SECONDARY = "#883039"
COLOR_UPPER_SECONDARY = "#578145"
COLOR_TERTIARY = "#B16214"

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "originUrl": "ourworldindata.org/education",
    "hideAnnotationFieldsInTitle": {"time": True},
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

# Column patterns for education spending indicators
SPENDING_PATTERNS = {
    "gdp_share": [
        "government_expenditure_on_pre_primary_education_as_a_percentage_of_gdp__pct",
        "government_expenditure_on_primary_education_as_a_percentage_of_gdp__pct",
        "government_expenditure_on_lower_secondary_education_as_a_percentage_of_gdp__pct",
        "government_expenditure_on_upper_secondary_education_as_a_percentage_of_gdp__pct",
        "government_expenditure_on_tertiary_education_as_a_percentage_of_gdp__pct",
        "combined_expenditure_share_gdp",
    ],
    "constant_ppp": [
        "government_expenditure_on_pre_primary_education__constant_pppdollar__millions",
        "government_expenditure_on_primary_education__constant_pppdollar__millions",
        "government_expenditure_on_lower_secondary_education__constant_pppdollar__millions",
        "government_expenditure_on_upper_secondary_education__constant_pppdollar__millions",
        "government_expenditure_on_tertiary_education__constant_pppdollar__millions",
        "government_expenditure_on_education__constant_pppdollar__millions",
    ],
    "total_government": [
        "expenditure_on_education_as_a_percentage_of_total_government_expenditure__pct__xgovexp_imf",
    ],
    "per_student": [
        "initial_government_funding_per_pre_primary_student__constant_pppdollar__xunit_pppconst_02_fsgov_ffntr",
        "initial_government_funding_per_primary_student__constant_pppdollar__xunit_pppconst_1_fsgov_ffntr",
        "initial_government_funding_per_lower_secondary_student__constant_pppdollar__xunit_pppconst_2_fsgov_ffntr",
        "initial_government_funding_per_upper_secondary_student__constant_pppdollar__xunit_pppconst_3_fsgov_ffntr",
        "initial_government_funding_per_tertiary_student__constant_pppdollar__xunit_pppconst_5t8_fsgov_ffntr",
        "government_expenditure_on_education_per_student__total_across_all_levels__constant_pppdollar",
    ],
}


def run() -> None:
    """Main function to process education spending data and create collection."""
    # Load inputs
    config = paths.load_collection_config()

    # Load datasets
    ds_opri = paths.load_dataset("education_opri")
    ds_sdgs = paths.load_dataset("education_sdgs")

    tb_opri = ds_opri.read("education_opri", load_data=False)
    tb_sdgs = ds_sdgs.read("education_sdgs", load_data=False)

    # Get spending columns from both datasets
    spending_cols_opri = get_spending_columns(tb_opri)
    spending_cols_sdgs = get_spending_columns(tb_sdgs)

    # Select relevant columns and combine tables
    tb_opri = tb_opri.loc[:, ["country", "year"] + spending_cols_opri].copy()
    tb_sdgs = tb_sdgs.loc[:, ["country", "year"] + spending_cols_sdgs].copy()

    # Adjust dimensions
    tb_opri = adjust_dimensions(tb_opri)
    tb_sdgs = adjust_dimensions(tb_sdgs)

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=[tb_opri, tb_sdgs],
        common_view_config=MULTIDIM_CONFIG,
    )

    # Add grouped views
    create_grouped_views(c)

    # Edit FAUST
    c.set_global_config(
        config={
            "title": lambda view: generate_title_by_spending_type_and_level(view),
        }
    )

    # Edit display names
    for view in c.views:
        edit_indicator_displays(view)

    # Save collection
    c.save()


def get_spending_columns(tb):
    """Filter education spending columns."""
    spending_cols = []

    for pattern_list in SPENDING_PATTERNS.values():
        for pattern in pattern_list:
            matching_cols = [col for col in tb.columns if pattern in col]
            spending_cols.extend(matching_cols)

    return spending_cols


def adjust_dimensions(tb):
    """Add dimensions to education spending table columns."""

    # Dimension mapping configurations
    LEVEL_KEYWORDS = {
        "expenditure_on_pre_primary": "preprimary",
        "per_pre_primary_student": "preprimary",
        "expenditure_on_primary": "primary",
        "per_primary_student": "primary",
        "lower_secondary": "lower_secondary",
        "upper_secondary": "upper_secondary",
        "tertiary": "tertiary",
    }

    SPENDING_TYPE_KEYWORDS = {
        "percentage_of_gdp": "gdp_share",
        "share_gdp": "gdp_share",
        "constant_pppdollar__millions": "constant_ppp",
        "total_government_expenditure": "total_government",
        "per_student": "per_student",
        "initial_government_funding": "per_student",
    }

    # Process each column
    for col in tb.columns:
        if col in ["country", "year"]:
            continue

        # Extract education level
        level = None
        for keyword, value in LEVEL_KEYWORDS.items():
            if keyword in col:
                level = value
                break

        # Extract spending type
        spending_type = None
        for keyword, value in SPENDING_TYPE_KEYWORDS.items():
            if keyword in col:
                spending_type = value
                break

        # Default spending type based on column patterns
        if spending_type is None:
            if "per_student" in col:
                spending_type = "per_student"
            elif "percentage_of_gdp" in col or "__pct" in col:
                spending_type = "gdp_share"
            elif "constant_pppdollar" in col:
                spending_type = "constant_ppp"
            elif "total_government" in col:
                spending_type = "total_government"
            elif "combined" in col:
                spending_type = "gdp_share"

        # Set indicator name
        tb[col].metadata.original_short_name = "education_spending"

        # Set dimensions
        tb[col].metadata.dimensions = {
            "level": level or "all",
            "spending_type": spending_type or "gdp_share",
        }
    # Add dimension definitions at table level
    if not hasattr(tb.metadata, "dimensions") or tb.metadata.dimensions is None:
        tb.metadata.dimensions = []

    # Add dimension definitions to table metadata
    tb.metadata.dimensions.extend(
        [
            {"name": "Education level", "slug": "level"},
            {"name": "Spending type", "slug": "spending_type"},
        ]
    )

    return tb


def create_grouped_views(collection):
    """Add grouped views for education level comparisons."""
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
                "dimension": "level",
                "choice_new_slug": "level_side_by_side",
                "choices": {"preprimary", "primary", "lower_secondary", "upper_secondary", "tertiary"},
                "view_config": view_config,
                "view_metadata": view_metadata,
            },
        ],
        params={
            "title": lambda view: generate_title_by_spending_type_and_level(view),
            "subtitle": lambda view: generate_subtitle_by_spending_type_and_level(view),
        },
    )


# Common mappings used by both title and subtitle functions
SPENDING_TYPE_MAPPINGS = {
    "title": {
        "gdp_share": "as a share of GDP",
        "constant_ppp": "total spending",
        "total_government": "as a share of total spending",
        "per_student": "per student",
    },
    "subtitle": {
        "gdp_share": "expressed as a percentage of [GDP](#dod:gdp)",
        "constant_ppp": "expressed in constant [international-$](#dod:int_dollar_abbreviation)",
        "total_government": "expressed as a percentage of total government expenditure",
        "per_student": "per student, expressed in constant [international-$](#dod:int_dollar_abbreviation)",
    },
}

LEVEL_MAPPINGS = {
    "title": {
        "preprimary": "pre-primary education",
        "primary": "primary education",
        "lower_secondary": "lower secondary education",
        "upper_secondary": "upper secondary education",
        "tertiary": "tertiary education",
        "all": "education",
        "level_side_by_side": "education by level",
    },
    "subtitle": {
        "preprimary": "[pre-primary](#dod:pre-primary-education) education,",
        "primary": "[primary](#dod:primary-education) education,",
        "lower_secondary": "[lower secondary](#dod:lower-secondary-education) education,",
        "upper_secondary": "[upper secondary](#dod:upper-secondary-education) education,",
        "tertiary": "[tertiary](#dod:tertiary-education) education,",
        "all": "education across all levels,",
        "level_side_by_side": "[pre-primary](#dod:pre-primary-education), [primary](#dod:primary-education), [lower secondary](#dod:lower-secondary-education), [upper secondary](#dod:upper-secondary-education), and [tertiary](#dod:tertiary-education) education,",
    },
}


def generate_title_by_spending_type_and_level(view):
    """Generate title based on spending type and education level."""
    spending_type, level = view.dimensions["spending_type"], view.dimensions["level"]

    # Get spending type term
    spending_term = SPENDING_TYPE_MAPPINGS["title"].get(spending_type, "")
    # Get level term
    level_term = LEVEL_MAPPINGS["title"].get(level, "")

    if not spending_term:
        raise ValueError(f"Unknown spending type: {spending_type}")
    if not level_term:
        raise ValueError(f"Unknown education level: {level}")
    if spending_term == "total spending":
        return f"Total government spending on {level_term}"
    elif spending_term == "per student":
        return f"Government spending on {level_term} per student"

    return f"Government spending on {level_term} {spending_term}"


def generate_subtitle_by_spending_type_and_level(view):
    """Generate subtitle based on spending type and education level with links."""
    spending_type, level = view.dimensions["spending_type"], view.dimensions["level"]

    spending_term = SPENDING_TYPE_MAPPINGS["subtitle"].get(spending_type, "")
    level_term = LEVEL_MAPPINGS["subtitle"].get(level, "")

    if not spending_term:
        raise ValueError(f"Unknown spending type: {spending_type}")
    if not level_term:
        raise ValueError(f"Unknown education level: {level}")

    return f"Total [general government](#dod:general-government) spending on {level_term} {spending_term}."


def edit_indicator_displays(view):
    """Edit display names and colors for the grouped views."""
    if view.indicators.y is None:
        return

    # Display name and color mappings for education levels
    LEVEL_CONFIG = {
        "pre_primary": {"name": "Pre-primary", "color": COLOR_PREPRIMARY},
        "primary": {"name": "Primary", "color": COLOR_PRIMARY},
        "lower_secondary": {"name": "Lower secondary", "color": COLOR_LOWER_SECONDARY},
        "upper_secondary": {"name": "Upper secondary", "color": COLOR_UPPER_SECONDARY},
        "tertiary": {"name": "Tertiary", "color": COLOR_TERTIARY},
    }

    # Check for level-based display names
    if view.matches(level="level_side_by_side"):
        for indicator in view.indicators.y:
            for level_key, config in LEVEL_CONFIG.items():
                if level_key in indicator.catalogPath:
                    indicator.display = {"name": config["name"], "color": config["color"]}
                    break

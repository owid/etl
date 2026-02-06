"""Load a meadow dataset and create a garden dataset."""

from etl.collection import combine_config_dimensions, expand_config
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP year
PPP_YEAR = 2021

# Define indicators to use
INDICATORS = ["mean", "median", "avg", "thr", "share"]

# Define dimensions for main views
DIMENSIONS_CONFIG = {
    "decile": "*",
    "period": "*",
    "table": ["Income or consumption consolidated", "Income with spells", "Consumption with spells"],
    "survey_comparability": "*",
}

PPP_ADJUSTMENT_SUBTITLE = "This data is adjusted for inflation and differences in living costs between countries."


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("world_bank_pip")
    tb = ds.read("incomes", load_data=False)

    # Remove unwanted dimensions.
    # NOTE: This is a temporary solution until we figure out how to deal with missing dimensions.
    columns_to_keep = []
    for column in tb.drop(columns=["country", "year"]).columns:
        # Keep only indicators for a specific PPP year, and then remove that dimension.
        if ("ppp_version" in tb[column].metadata.dimensions) and tb[column].metadata.dimensions[
            "ppp_version"
        ] == PPP_YEAR:
            columns_to_keep.append(column)
            tb[column].metadata.dimensions.pop("ppp_version")

        # Remove dimensions that are not needed.
        for dimension in ["welfare_type"]:
            if dimension in tb[column].metadata.dimensions:
                tb[column].metadata.dimensions.pop(dimension)
    tb = tb[columns_to_keep]

    # Get all survey_comparability values except "No spells" for spell views
    survey_comp_values = set()
    for col in tb.columns:
        if "survey_comparability" in tb[col].metadata.dimensions:
            survey_comp_values.add(tb[col].metadata.dimensions["survey_comparability"])
    survey_comp_spells = [v for v in survey_comp_values if v != "No spells"]

    # Bake config automatically from table
    config_new = expand_config(
        tb,  # type: ignore
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # Combine both sources (YAML dimensions + auto-generated dimensions)
    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] += config_new["views"]

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="incomes_pip",
    )

    # First, group survey_comparability (this must happen first)
    c.group_views(
        groups=[
            {
                "dimension": "survey_comparability",
                "choices": survey_comp_spells,
                "choice_new_slug": "Spells",
                "replace": True,
                "view_config": {
                    "hideRelativeToggle": True,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                },
            },
        ],
    )

    # Then, group the table dimension
    c.group_views(
        groups=[
            {
                "dimension": "table",
                "choices": ["Income with spells", "Consumption with spells"],
                "choice_new_slug": "Income or consumption consolidated",
                "replace": True,
                "view_config": {
                    "hideRelativeToggle": True,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                },
            },
        ],
    )

    # Group all deciles together (only for avg, thr, share - not mean/median)
    decile_choices = c.get_choice_names("decile")
    decile_values = [
        slug
        for slug, name in decile_choices.items()
        if name and slug not in ("all", "all_bar", "10_40_50", "10_40_50_bar")
    ]
    c.group_views(
        groups=[
            {
                "dimension": "decile",
                "choices": decile_values,
                "choice_new_slug": "all",
                "view_config": {
                    "hideRelativeToggle": True,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                    "baseColorScheme": "OwidCategoricalE",
                },
            },
        ],
    )

    # Group all deciles together as bar chart
    c.group_views(
        groups=[
            {
                "dimension": "decile",
                "choices": decile_values,
                "choice_new_slug": "all_bar",
                "view_config": {
                    "hideRelativeToggle": True,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["StackedDiscreteBar"],
                    "hideTotalValueLabel": True,
                    "baseColorScheme": "OwidCategoricalE",
                },
            },
        ],
    )

    # Fix the "all" and "all_bar" choice names and set groups (group_views sets it to the slug since it wasn't in the dimension)
    decile_dim = c.get_dimension("decile")
    for choice in decile_dim.choices:
        if choice.slug == "all":
            choice.name = "All deciles"
            choice.group = "Compare different deciles"
        elif choice.slug == "all_bar":
            choice.name = "All deciles (bar chart)"
            choice.group = "Compare different deciles"

    # Filter decile views: keep only 1, 10, all for all indicators, plus 5, 9 for thr only
    # Also remove grouped decile views for Spells (we don't want those)
    non_share = [i for i in c.dimension_choices["indicator"] if i != "share"]
    non_thr = [i for i in c.dimension_choices["indicator"] if i != "thr"]
    c.drop_views(
        [
            {"decile": ["2", "3", "4", "6", "7", "8"]},
            {"decile": ["all_bar", "10_40_50", "10_40_50_bar"], "indicator": non_share},
            {"decile": ["5", "9"], "indicator": non_thr},
            {"decile": ["all", "all_bar"], "survey_comparability": "Spells"},
        ]
    )

    # Update chart type for share indicator grouped views to StackedArea
    for view in c.views:
        if view.matches(decile="all", indicator="share"):
            if view.config is None:
                view.config = {}
            view.config["chartTypes"] = ["StackedArea"]

    # Build mapping of catalogPath to display name from table metadata
    indicator_display_names = _build_indicator_display_names(tb)

    # For "all" and "all_bar" decile views, clean up indicator display names, sort by decile, and set titles
    for view in c.views:
        if (view.matches(decile="all") or view.matches(decile="all_bar")) and view.indicators.y:
            # Sort indicators by decile number
            # For share: richest to poorest; for others: poorest to richest
            # For all_bar: inverse order
            reverse_order = view.matches(indicator="share")
            if view.matches(decile="all_bar"):
                reverse_order = not reverse_order
            view.indicators.y = sorted(view.indicators.y, key=_get_decile_number, reverse=reverse_order)

            # For all_bar views, set sortBy to column and sortColumnSlug to decile 10 indicator
            if view.matches(decile="all_bar"):
                decile_10_ind = next((ind for ind in view.indicators.y if _get_decile_number(ind) == 10), None)
                if decile_10_ind:
                    if view.config is None:
                        view.config = {}
                    view.config["sortBy"] = "column"
                    view.config["sortColumnSlug"] = decile_10_ind.catalogPath

            # Set display names extracted from original indicator titles
            for ind in view.indicators.y:
                name = _get_display_name_from_metadata(ind, indicator_display_names)
                if name:
                    ind.display = {"name": name}

            # Set titles and subtitles based on indicator type
            period = view.dimensions.get("period")
            if view.config is None:
                view.config = {}

            if view.matches(indicator="thr"):
                view.config["title"] = f"Threshold income or consumption per {period} for each decile"
                subtitle = f"The level of after tax income or consumption per person per {period} below which 10%, 20%, 30%, etc. of the population falls. {PPP_ADJUSTMENT_SUBTITLE}"
                view.config["subtitle"] = subtitle
                view.metadata = {"description_short": subtitle}
            elif view.matches(indicator="avg"):
                view.config["title"] = f"Mean income or consumption per {period} within each decile"
                subtitle = f"The mean after tax income or consumption per person per {period} within each decile (tenth of the population). {PPP_ADJUSTMENT_SUBTITLE}"
                view.config["subtitle"] = subtitle
                view.metadata = {"description_short": subtitle}
            elif view.matches(indicator="share"):
                view.config["title"] = "Income or consumption share for each decile"
                subtitle = (
                    "The share of after tax income or consumption received by each decile (tenth of the population)."
                )
                view.config["subtitle"] = subtitle
                view.metadata = {"description_short": subtitle}

    #
    # Save garden dataset.
    #
    c.save()



def _build_indicator_display_names(tb):
    """Build mapping of column names to display names from table metadata."""
    indicator_display_names = {}
    for col in tb.columns:
        if col not in ["country", "year"]:
            display_name = tb[col].metadata.display.get("name", "") if tb[col].metadata.display else ""
            if display_name:
                indicator_display_names[col] = display_name
    return indicator_display_names


def _get_decile_number(ind):
    """Extract decile number from indicator catalogPath."""
    # Check in reverse order to avoid decile_1 matching decile_10
    for i in range(10, 0, -1):
        if f"decile_{i}__" in ind.catalogPath or ind.catalogPath.endswith(f"decile_{i}"):
            return i
    return 0


def _get_display_name_from_metadata(ind, indicator_titles):
    """Get display name from original indicator metadata, extracting text between parentheses."""
    col_name = ind.catalogPath.split("#")[-1] if "#" in ind.catalogPath else None
    if col_name and col_name in indicator_titles:
        text = indicator_titles[col_name]
        start, end = text.find("("), text.find(")")
        if start != -1 and end != -1:
            extracted = text[start + 1 : end]
            return extracted[0].upper() + extracted[1:] if extracted else extracted
    return None

"""Load a meadow dataset and create a garden dataset."""

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

# Set x (population) and color (region) indicators needed by the Marimekko tab.
POPULATION_PATH = "grapher/demography/2024-07-15/population/historical#population_historical"
REGION_PATH = "grapher/regions/2023-01-01/regions/regions#owid_region"


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

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="incomes_pip",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
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
        if name
        and slug
        not in (
            "all",
            "10_40_50",
        )
    ]
    c.group_views(
        groups=[
            {
                "dimension": "decile",
                "choices": decile_values,
                "choice_new_slug": "all",
                "view_config": {
                    "hideRelativeToggle": False,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": lambda view: (
                        ["StackedArea", "StackedDiscreteBar"] if view.matches(indicator="share") else ["LineChart"]
                    ),
                    "hideTotalValueLabel": True,
                    "baseColorScheme": "OwidCategoricalE",
                    "title": "{title}",
                    "subtitle": "{subtitle}",
                },
                "view_metadata": {
                    "description_short": "{subtitle}",
                },
            },
        ],
        params={
            "title": _get_grouped_decile_title,
            "subtitle": _get_grouped_decile_subtitle,
        },
    )

    # Group deciles 1, 5, 9 as P10/P50/P90 — only used for thr indicator
    c.group_views(
        groups=[
            {
                "dimension": "decile",
                "choices": ["1", "5", "9"],
                "choice_new_slug": "p10_p50_p90",
                "view_config": {
                    "hideRelativeToggle": False,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                    "hideTotalValueLabel": True,
                    "baseColorScheme": "OwidCategoricalE",
                    "title": "{title}",
                    "subtitle": "{subtitle}",
                },
                "view_metadata": {
                    "description_short": "{subtitle}",
                },
            },
        ],
        params={
            "title": _get_p10_p50_p90_title,
            "subtitle": _get_p10_p50_p90_subtitle,
        },
    )

    # Filter decile views: keep only 1, 10, all for all indicators, plus 5, 9 for thr only
    # Also remove grouped decile views for Spells (we don't want those)
    non_share = [i for i in c.dimension_choices["indicator"] if i != "share"]
    non_thr = [i for i in c.dimension_choices["indicator"] if i != "thr"]
    c.drop_views(
        [
            {"decile": ["2", "3", "4", "6", "7", "8"]},
            {"decile": ["10_40_50"], "indicator": non_share},
            {"decile": ["5", "9"], "indicator": non_thr},
            {"decile": ["all"], "survey_comparability": "Spells"},
            {"decile": ["p10_p50_p90"], "indicator": non_thr},
            {"decile": ["p10_p50_p90"], "survey_comparability": "Spells"},
        ]
    )

    # Build mapping of catalogPath to display name from table metadata
    indicator_display_names = _build_indicator_display_names(tb)

    # Customize grouped decile views: sort indicators and set display names
    for view in c.views:
        if view.matches(decile=["all", "p10_p50_p90"]) and view.indicators.y:
            # Sort indicators by decile number
            # For share: richest to poorest; for others: poorest to richest
            reverse_order = view.matches(indicator="share")
            view.indicators.y = sorted(view.indicators.y, key=_get_decile_number, reverse=reverse_order)

            # Set sortBy to last indicator in the list
            view.config = view.config or {}
            view.config["sortBy"] = "column"
            view.config["sortColumnSlug"] = view.indicators.y[0].catalogPath

            # Set display names extracted from original indicator titles
            for ind in view.indicators.y:
                name = _get_display_name_from_metadata(ind, indicator_display_names)
                if name:
                    ind.display = {"name": name}

    # Add Marimekko as an additional chart type for mean and median views.
    for view in c.views:
        if view.matches(survey_comparability="No spells") and not view.matches(
            decile=["all", "10_40_50", "p10_p50_p90"]
        ):
            view.config = view.config or {}
            view.config["chartTypes"] = ["LineChart", "DiscreteBar", "Marimekko"]
            view.indicators.set_indicator(
                x=POPULATION_PATH,
                color=REGION_PATH,
            )
            view.config["matchingEntitiesOnly"] = True

    #
    # Save garden dataset.
    #
    c.save()


def _get_grouped_decile_title(view):
    """Return title for grouped decile views based on indicator type."""
    period = view.dimensions.get("period")
    titles = {
        "thr": f"Threshold income or consumption per {period} for each decile",
        "avg": f"Mean income or consumption per {period} within each decile",
        "share": "Income or consumption share for each decile",
    }
    return titles.get(view.dimensions.get("indicator"), "")


def _get_grouped_decile_subtitle(view):
    """Return subtitle for grouped decile views based on indicator type."""
    period = view.dimensions.get("period")
    subtitles = {
        "thr": f"The level of after tax income or consumption per person per {period} below which 10%, 20%, 30%, etc. of the population falls. {PPP_ADJUSTMENT_SUBTITLE}",
        "avg": f"The mean after tax income or consumption per person per {period} within each decile (tenth of the population). {PPP_ADJUSTMENT_SUBTITLE}",
        "share": "The share of after tax income or consumption received by each decile (tenth of the population).",
    }
    return subtitles.get(view.dimensions.get("indicator"), "")


def _get_p10_p50_p90_title(view):
    """Return title for the P10/P50/P90 grouped threshold view."""
    period = view.dimensions.get("period")
    return f"Threshold income or consumption per {period} marking marking the poorest decile, the median, and the richest decile"


def _get_p10_p50_p90_subtitle(view):
    """Return subtitle for the P10/P50/P90 grouped threshold view."""
    period = view.dimensions.get("period")
    return (
        f"The level of after tax income or consumption per person per {period} below which 10%, 50% and 90% of the population falls. "
        f"{PPP_ADJUSTMENT_SUBTITLE}"
    )


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

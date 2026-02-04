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
                "overwrite_dimension_choice": True,
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
    decile_values = [slug for slug, name in decile_choices.items() if name and slug != "all"]
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
                },
            },
        ],
    )

    # Fix the "all" choice name (group_views sets it to the slug since it wasn't in the dimension)
    decile_dim = c.get_dimension("decile")
    for choice in decile_dim.choices:
        if choice.slug == "all":
            choice.name = "All deciles"
            break

    # Filter decile views: keep only 1, 10, all for all indicators, plus 5, 9 for thr only
    # Also remove grouped decile views for Spells (we don't want those)
    c.views = [v for v in c.views if _keep_decile_view(v) and not v.matches(decile="all", survey_comparability="Spells")]

    # Update chart type for share indicator grouped views to StackedArea
    for view in c.views:
        if view.matches(decile="all", indicator="share"):
            if view.config is None:
                view.config = {}
            view.config["chartTypes"] = ["StackedArea"]

    # Build mapping of catalogPath to display name from table metadata
    indicator_titles = _build_indicator_titles(tb)

    # For "all" decile views, clean up indicator display names and sort by decile
    for view in c.views:
        if view.matches(decile="all") and view.indicators.y:

            # Sort indicators by decile number
            # For share: richest to poorest; for others: poorest to richest
            reverse_order = view.matches(indicator="share")
            view.indicators.y = sorted(view.indicators.y, key=_get_decile_number, reverse=reverse_order)

            # Set display names extracted from original indicator titles
            for ind in view.indicators.y:
                name = _get_display_name_from_metadata(ind, indicator_titles)
                if name:
                    ind.display = {"name": name}

    #
    # Save garden dataset.
    #
    c.save()


def _keep_decile_view(v):
    """Filter decile views: keep only 1, 10, all for all indicators, plus 5, 9 for thr only."""
    decile = v.dimensions.get("decile")
    indicator = v.dimensions.get("indicator")
    # Keep nan decile (for mean/median which don't have decile data)
    if decile in ["nan", "all"]:
        return True
    # Keep deciles 1 and 10 for all indicators
    if decile in ["1", "10"]:
        return True
    # Keep deciles 5 and 9 only for thr indicator
    if decile in ["5", "9"] and indicator == "thr":
        return True
    return False


def _build_indicator_titles(tb):
    """Build mapping of column names to display names from table metadata."""
    indicator_titles = {}
    for col in tb.columns:
        if col not in ["country", "year"]:
            display_name = tb[col].metadata.display.get("name", "") if tb[col].metadata.display else ""
            if display_name:
                indicator_titles[col] = display_name
    return indicator_titles


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

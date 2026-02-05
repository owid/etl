"""Load a meadow dataset and create a garden dataset."""

from etl.collection import combine_config_dimensions, expand_config
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP year
PPP_YEAR = 2021

# Define indicators to use
INDICATORS = ["share"]

# Define dimensions for main views
DIMENSIONS_CONFIG = {
    "welfare_type": ["before tax", "after tax"],
    "quantile": "*",
    "period": "*",
    "extrapolated": "no",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("world_inequality_database")
    tb = ds.read("incomes", load_data=False)

    # Remove unwanted dimensions.
    # NOTE: This is a temporary solution until we figure out how to deal with missing dimensions.
    for column in tb.drop(columns=["country", "year"]).columns:
        # Remove dimensions that are not needed.
        for dimension in ["period", "extrapolated"]:
            if dimension in tb[column].metadata.dimensions:
                tb[column].metadata.dimensions.pop(dimension)

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
        short_name="incomes_wid",
    )

    # Group all deciles together (only for avg, thr, share - not mean/median)
    decile_choices = c.get_choice_names("quantile")
    decile_values = [slug for slug, name in decile_choices.items() if name and slug not in ("all", "10_40_50")]
    c.group_views(
        groups=[
            {
                "dimension": "quantile",
                "choices": decile_values,
                "choice_new_slug": "all",
                "view_config": {
                    "hideRelativeToggle": True,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["StackedArea"],
                },
            },
        ],
    )

    # Fix the "all" choice name (group_views sets it to the slug since it wasn't in the dimension)
    decile_dim = c.get_dimension("quantile")
    for choice in decile_dim.choices:
        if choice.slug == "all":
            choice.name = "All deciles"
            break

    # Filter decile views: keep only 1, 10, all for all indicators, plus 5, 9 for thr only
    # Also remove grouped decile views for Spells (we don't want those)
    c.views = [
        v for v in c.views if v.dimensions.get("quantile") in ["Richest 0.1%", "Richest 1%", "10", "10_40_50", "all"]
    ]

    # Build mapping of catalogPath to display name from table metadata
    indicator_display_names = _build_indicator_display_names(tb)

    # For "all" decile views, clean up indicator display names, sort by decile, and set titles
    for view in c.views:
        if view.matches(quantile="all") and view.indicators.y:
            # Sort indicators by decile number
            # For share: richest to poorest; for others: poorest to richest
            reverse_order = view.matches(indicator="share")
            view.indicators.y = sorted(view.indicators.y, key=_get_decile_number, reverse=reverse_order)

            # Set display names extracted from original indicator titles
            for ind in view.indicators.y:
                name = _get_display_name_from_metadata(ind, indicator_display_names)
                if name:
                    ind.display = {"name": name}

            # Set titles and subtitles based on indicator type
            welfare_type = view.dimensions.get("welfare_type")
            if view.config is None:
                view.config = {}

            view.config["title"] = f"Income share for each decile ({welfare_type})"
            subtitle = f"The share of income received by each decile (tenth of the population). Income here is measured {welfare_type}es and benefits."
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

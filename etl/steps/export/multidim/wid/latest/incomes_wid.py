"""Load a meadow dataset and create a garden dataset."""

from etl.collection import combine_config_dimensions, expand_config
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP year
PPP_YEAR = 2021

# Define indicators to use
INDICATORS = ["share"]

# Define dimensions for main views (period and extrapolated are filtered out, not user-selectable)
DIMENSIONS_CONFIG = {
    "welfare_type": ["before tax", "after tax"],
    "quantile": "*",
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

    # Filter columns to only keep extrapolated=no, then remove that dimension from metadata.
    # Also keep only specific periods and remove that dimension too.
    columns_to_keep = []
    for column in tb.drop(columns=["country", "year"]).columns:
        dims = tb[column].metadata.dimensions
        if dims and dims.get("extrapolated") == "no":
            columns_to_keep.append(column)
            # Remove dimensions that are not needed (they're now fixed values)
            for dimension in ["period", "extrapolated"]:
                if dimension in dims:
                    dims.pop(dimension)
    tb = tb[columns_to_keep]

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

    # Group deciles 1-10 together into an "all" view
    decile_values = [str(i) for i in range(1, 11)]
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
                    "baseColorScheme": "OwidCategoricalE",
                },
            },
        ],
    )

    # Group deciles 1-10 together as bar chart
    c.group_views(
        groups=[
            {
                "dimension": "quantile",
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

    # Fix the "all" and "all_bar" choice names (group_views sets it to the slug since it wasn't in the dimension)
    decile_dim = c.get_dimension("quantile")
    for choice in decile_dim.choices:
        if choice.slug == "all":
            choice.name = "All deciles"
        elif choice.slug == "all_bar":
            choice.name = "All deciles (bar chart)"

    # Filter quantile views: keep only specific quantiles
    c.views = [
        v
        for v in c.views
        if v.dimensions.get("quantile")
        in ["Richest 0.1%", "Richest 1%", "10", "10_40_50", "10_40_50_bar", "all", "all_bar"]
    ]

    # Build mapping of catalogPath to display name from table metadata
    indicator_display_names = _build_indicator_display_names(tb)

    # For "all" and "all_bar" decile views, clean up indicator display names, sort by decile, and set titles
    for view in c.views:
        quantile = view.dimensions.get("quantile")
        if quantile in ["all", "all_bar"] and view.indicators.y:
            # Sort indicators by decile number
            # For all: richest to poorest; for all_bar: poorest to richest
            reverse_order = quantile == "all"
            view.indicators.y = sorted(view.indicators.y, key=_get_decile_number, reverse=reverse_order)

            # For all_bar views, set sortBy to column and sortColumnSlug to decile 10 indicator
            if quantile == "all_bar":
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
            welfare_type = view.dimensions.get("welfare_type")
            if view.config is None:
                view.config = {}

            view.config["title"] = f"Income share for each decile ({welfare_type})"
            subtitle = f"The share of income received by each decile (tenth of the population). Income here is measured {welfare_type}es and benefits."
            view.config["subtitle"] = subtitle
            view.metadata = {"description_short": subtitle}

    # Group welfare_type (before vs after tax) for specific quantiles only
    c.group_views(
        groups=[
            {
                "dimension": "welfare_type",
                "choices": ["before tax", "after tax"],
                "choice_new_slug": "before_vs_after",
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

    # Remove grouped welfare_type views for quantiles we don't want grouped (keep only Richest 0.1%, Richest 1%, 10)
    c.views = [
        v
        for v in c.views
        if v.dimensions.get("welfare_type") != "before_vs_after"
        or v.dimensions.get("quantile") in ["Richest 0.1%", "Richest 1%", "10"]
    ]

    # Customize grouped welfare_type views (before_vs_after)
    for view in c.views:
        if view.dimensions.get("welfare_type") == "before_vs_after" and view.indicators.y:
            # Get metadata from first indicator
            first_ind = view.indicators.y[0]
            col_name = first_ind.catalogPath.split("#")[-1] if "#" in first_ind.catalogPath else None

            if col_name and col_name in tb.columns:
                meta = tb[col_name].metadata
                grapher_config = meta.presentation.grapher_config if meta.presentation else {}

                # Extract and modify title
                title = grapher_config.get("title", "")
                title = title.replace("before tax", "before vs. after tax")

                # Extract and modify subtitle (remove welfare type phrase)
                subtitle = grapher_config.get("subtitle", "")
                subtitle = subtitle.replace(" Income here is measured before taxes and benefits.", "")

                # Extract and modify description_short (remove welfare type phrase)
                description_short = meta.description_short or ""
                description_short = description_short.replace(" Income here is measured before taxes and benefits.", "")

                # Get description_key and remove first element
                description_key = list(meta.description_key) if meta.description_key else []
                if description_key:
                    description_key = description_key[1:]

                # Set config
                view.config = {
                    "title": title,
                    "subtitle": subtitle,
                    "note": "",
                    "hideRelativeToggle": True,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                }

                # Set metadata
                view.metadata = {
                    "description_short": description_short,
                    "description_key": description_key,
                }

            # Set display names based on indicator (before tax or after tax)
            for ind in view.indicators.y:
                if "before_tax" in ind.catalogPath:
                    ind.display = {"name": "Before tax"}
                elif "after_tax" in ind.catalogPath:
                    ind.display = {"name": "After tax"}

    # Group welfare_type (before vs after tax) as scatter plot for specific quantiles
    c.group_views(
        groups=[
            {
                "dimension": "welfare_type",
                "choices": ["before tax", "after tax"],
                "choice_new_slug": "before_vs_after_scatter",
                "view_config": {
                    "hideRelativeToggle": True,
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["ScatterPlot"],
                },
            },
        ],
    )

    # Remove grouped scatter views for quantiles we don't want (keep only Richest 0.1%, Richest 1%, 10)
    c.views = [
        v
        for v in c.views
        if v.dimensions.get("welfare_type") != "before_vs_after_scatter"
        or v.dimensions.get("quantile") in ["Richest 0.1%", "Richest 1%", "10"]
    ]

    # Customize scatter plot views: move before_tax to x axis, keep after_tax on y
    for view in c.views:
        if view.dimensions.get("welfare_type") == "before_vs_after_scatter" and view.indicators.y:
            # Find before_tax and after_tax indicators
            before_tax_ind = None
            after_tax_ind = None
            for ind in view.indicators.y:
                if "before_tax" in ind.catalogPath:
                    before_tax_ind = ind
                elif "after_tax" in ind.catalogPath:
                    after_tax_ind = ind

            # Set x to before_tax, y to after_tax, size to population, color to region
            if before_tax_ind and after_tax_ind:
                from etl.collection.model.view import Indicator

                before_tax_ind.display = {"name": "Before tax"}
                after_tax_ind.display = {"name": "After tax"}
                view.indicators.x = before_tax_ind
                view.indicators.y = [after_tax_ind]
                view.indicators.size = Indicator(
                    catalogPath="grapher/demography/2024-07-15/population/historical#population_historical"
                )
                view.indicators.color = Indicator(
                    catalogPath="grapher/regions/2023-01-01/regions/regions#owid_region"
                )

            # Get metadata from after_tax indicator for title/subtitle
            if after_tax_ind:
                col_name = after_tax_ind.catalogPath.split("#")[-1] if "#" in after_tax_ind.catalogPath else None
                if col_name and col_name in tb.columns:
                    meta = tb[col_name].metadata
                    grapher_config = meta.presentation.grapher_config if meta.presentation else {}

                    # Extract and modify title
                    title = grapher_config.get("title", "")
                    title = title.replace("after tax", "before vs. after tax")

                    # Determine axis min based on quantile
                    quantile = view.dimensions.get("quantile")
                    axis_min_map = {
                        "Richest 0.1%": 0,
                        "Richest 1%": 5,
                        "10": 20,
                    }
                    axis_min = axis_min_map.get(quantile, 0)

                    # Set config
                    view.config = {
                        "title": title,
                        "subtitle": "Comparing the share of income before and after taxes and benefits.",
                        "hideRelativeToggle": True,
                        "hasMapTab": False,
                        "tab": "chart",
                        "chartTypes": ["ScatterPlot"],
                        "comparisonLines": [{"yEquals": "x"}, {"yEquals": "0.75*x", "label": "25% reduction"}, {"yEquals": "0.5*x", "label": "50% reduction"}],
                        "matchingEntitiesOnly": True,
                        "minTime": "latest",
                        "xAxis": {"min": axis_min},
                        "yAxis": {"min": axis_min},
                    }

                    # Set metadata
                    view.metadata = {
                        "description_short": "Comparing the share of income before and after taxes and benefits.",
                    }

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
    """Extract decile/quantile number from indicator catalogPath."""
    # Check in reverse order to avoid quantile_1 matching quantile_10
    for i in range(10, 0, -1):
        if f"quantile_{i}__" in ind.catalogPath or ind.catalogPath.endswith(f"quantile_{i}"):
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
            # Remove welfare type suffix if present
            for suffix in [", before tax", ", after tax"]:
                if extracted.endswith(suffix):
                    extracted = extracted[: -len(suffix)]
                    break
            return extracted[0].upper() + extracted[1:] if extracted else extracted
    return None

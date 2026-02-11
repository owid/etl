"""Load a meadow dataset and create a garden dataset."""

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

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="incomes_wid",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # Group deciles 1-10 together
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
                    "title": "{title}",
                    "subtitle": "{subtitle}",
                },
                "view_metadata": {"description_short": "{subtitle}"},
            },
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
                    "title": "{title}",
                    "subtitle": "{subtitle}",
                },
                "view_metadata": {"description_short": "{subtitle}"},
            },
        ],
        params={
            "title": _get_grouped_quantile_title,
            "subtitle": _get_grouped_quantile_subtitle,
        },
    )

    # Filter quantile views: keep only specific quantiles
    keep_quantiles = {"Richest 0.1%", "Richest 1%", "10", "10_40_50", "10_40_50_bar", "all", "all_bar"}
    c.drop_views({"quantile": [q for q in c.dimension_choices["quantile"] if q not in keep_quantiles]})

    # Build mapping of catalogPath to display name from table metadata
    indicator_display_names = _build_indicator_display_names(tb)

    # Customize grouped quantile views: sort indicators and set display names
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

    # Group welfare_type (before vs after tax) for specific quantiles only
    c.group_views(
        groups=[
            {
                "dimension": "welfare_type",
                "choices": ["before tax", "after tax"],
                "choice_new_slug": "before_vs_after",
                "view_config": {
                    "title": "{title}",
                    "subtitle": "{subtitle}",
                    "note": "",
                    "hideRelativeToggle": True,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                },
                "view_metadata": {
                    "description_short": lambda view: _get_before_vs_after_metadata(tb, view)["description_short"],
                    "description_key": lambda view: _get_before_vs_after_metadata(tb, view)["description_key"],
                },
            },
        ],
        params={
            "title": lambda view: _get_before_vs_after_metadata(tb, view)["title"],
            "subtitle": lambda view: _get_before_vs_after_metadata(tb, view)["subtitle"],
        },
    )

    # Remove grouped welfare_type views for quantiles we don't want grouped (keep only Richest 0.1%, Richest 1%, 10)
    c.drop_views({"welfare_type": ["before_vs_after"], "quantile": ["10_40_50", "10_40_50_bar", "all", "all_bar"]})

    # Set display names for before_vs_after views
    for view in c.views:
        if view.dimensions.get("welfare_type") == "before_vs_after" and view.indicators.y:
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
    c.drop_views(
        {"welfare_type": ["before_vs_after_scatter"], "quantile": ["10_40_50", "10_40_50_bar", "all", "all_bar"]}
    )

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
                view.indicators.color = Indicator(catalogPath="grapher/regions/2023-01-01/regions/regions#owid_region")

            # Get metadata from after_tax indicator for title/subtitle
            if after_tax_ind:
                col_name = after_tax_ind.catalogPath.split("#")[-1] if "#" in after_tax_ind.catalogPath else None
                if col_name and col_name in tb.columns:
                    meta = tb[col_name].metadata
                    grapher_config = meta.presentation.grapher_config if meta.presentation else {}

                    # Extract and modify title
                    title = grapher_config.get("title", "")
                    title = title.replace("after tax", "before vs. after tax")

                    # Extract and modify subtitle (remove welfare type phrase)
                    subtitle = grapher_config.get("subtitle", "")
                    subtitle = subtitle.replace(" Income here is measured after taxes and benefits.", "")

                    # Extract and modify description_short (remove welfare type phrase)
                    description_short = meta.description_short or ""
                    description_short = description_short.replace(
                        " Income here is measured after taxes and benefits.", ""
                    )

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
                        "subtitle": subtitle,
                        "hideRelativeToggle": True,
                        "hasMapTab": False,
                        "tab": "chart",
                        "chartTypes": ["ScatterPlot"],
                        "comparisonLines": [
                            {"yEquals": "x"},
                            {"yEquals": "0.75*x", "label": "25% reduction"},
                            {"yEquals": "0.5*x", "label": "50% reduction"},
                        ],
                        "matchingEntitiesOnly": True,
                        "minTime": "latest",
                        "xAxis": {"min": axis_min},
                        "yAxis": {"min": axis_min},
                    }

                    # Set metadata
                    view.metadata = {
                        "description_short": description_short,
                    }

    #
    # Save garden dataset.
    #
    c.save()


def _get_grouped_quantile_title(view):
    """Return title for grouped quantile views."""
    welfare_type = view.dimensions.get("welfare_type")
    return f"Income share for each decile ({welfare_type})"


def _get_grouped_quantile_subtitle(view):
    """Return subtitle for grouped quantile views."""
    welfare_type = view.dimensions.get("welfare_type")
    return f"The share of income received by each decile (tenth of the population). Income here is measured {welfare_type}es and benefits."


def _get_before_vs_after_metadata(tb, view):
    """Extract and transform metadata from grapher_config for before_vs_after views."""
    if not view.indicators.y:
        return {"title": "", "subtitle": "", "description_short": "", "description_key": []}

    first_ind = view.indicators.y[0]
    col_name = first_ind.catalogPath.split("#")[-1] if "#" in first_ind.catalogPath else None

    if col_name and col_name in tb.columns:
        meta = tb[col_name].metadata
        grapher_config = meta.presentation.grapher_config if meta.presentation else {}

        title = grapher_config.get("title", "")
        title = title.replace("before tax", "before vs. after tax")

        subtitle = grapher_config.get("subtitle", "")
        subtitle = subtitle.replace(" Income here is measured before taxes and benefits.", "")

        description_short = meta.description_short or ""
        description_short = description_short.replace(" Income here is measured before taxes and benefits.", "")

        description_key = list(meta.description_key) if meta.description_key else []
        if description_key:
            description_key = description_key[1:]

        return {
            "title": title,
            "subtitle": subtitle,
            "description_short": description_short,
            "description_key": description_key,
        }

    return {"title": "", "subtitle": "", "description_short": "", "description_key": []}


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

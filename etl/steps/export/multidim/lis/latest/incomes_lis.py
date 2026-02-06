"""Multidim export for LIS incomes across the distribution."""

from etl.collection import combine_config_dimensions, expand_config
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Define indicators to use
INDICATORS = ["mean", "median", "avg", "thr", "share"]

# Define dimensions for main views (equivalence_scale is filtered to "square root" and removed before expand)
DIMENSIONS_CONFIG = {
    "decile": "*",
    "period": "*",
    "welfare_type": ["dhi", "mi"],
}

PPP_ADJUSTMENT_SUBTITLE = "This data is adjusted for inflation and differences in living costs between countries."


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("luxembourg_income_study")
    tb = ds.read("incomes", load_data=False)

    # Filter to "square root" equivalence_scale and remove that dimension
    columns_to_keep = []
    for column in tb.drop(columns=["country", "year"]).columns:
        dims = tb[column].metadata.dimensions
        if dims and dims.get("equivalence_scale") == "square root":
            columns_to_keep.append(column)
            dims.pop("equivalence_scale")
            # Convert integer decile values to clean strings (e.g. 1 -> "1", not "1.0")
            if "decile" in dims and isinstance(dims["decile"], (int, float)):
                dims["decile"] = str(int(dims["decile"]))
    tb = tb[columns_to_keep]

    # Auto-generate config from table
    config_new = expand_config(
        tb,  # type: ignore
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # Combine YAML + auto-generated dimensions
    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] += config_new["views"]

    # Create collection
    c = paths.create_collection(
        config=config,
        short_name="incomes_lis",
    )

    # Group all deciles together
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

    # Group all deciles as bar chart
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

    # Filter decile views: keep only relevant deciles per indicator
    non_share = [i for i in c.dimension_choices["indicator"] if i != "share"]
    non_thr = [i for i in c.dimension_choices["indicator"] if i != "thr"]
    c.drop_views(
        [
            {"decile": ["2", "3", "4", "6", "7", "8"]},
            {"decile": ["all_bar", "10_40_50", "10_40_50_bar"], "indicator": non_share},
            {"decile": ["5", "9"], "indicator": non_thr},
        ]
    )

    # Update chart type for share "all" views to StackedArea
    for view in c.views:
        if view.matches(decile="all", indicator="share"):
            if view.config is None:
                view.config = {}
            view.config["chartTypes"] = ["StackedArea"]

    # Build indicator display names from metadata
    indicator_display_names = _build_indicator_display_names(tb)

    # Customize grouped decile views (all, all_bar)
    for view in c.views:
        if (view.matches(decile="all") or view.matches(decile="all_bar")) and view.indicators.y:
            # Sort indicators by decile number
            reverse_order = view.matches(indicator="share")
            if view.matches(decile="all_bar"):
                reverse_order = not reverse_order
            view.indicators.y = sorted(view.indicators.y, key=_get_decile_number, reverse=reverse_order)

            # For all_bar: set sortBy
            if view.matches(decile="all_bar"):
                decile_10_ind = next((ind for ind in view.indicators.y if _get_decile_number(ind) == 10), None)
                if decile_10_ind:
                    if view.config is None:
                        view.config = {}
                    view.config["sortBy"] = "column"
                    view.config["sortColumnSlug"] = decile_10_ind.catalogPath

            # Set display names
            for ind in view.indicators.y:
                name = _get_display_name_from_metadata(ind, indicator_display_names)
                if name:
                    ind.display = {"name": name}

            # Set titles/subtitles
            period = view.dimensions.get("period")
            welfare_type = view.dimensions.get("welfare_type")
            wt_label = "after tax" if welfare_type == "dhi" else "before tax"
            if view.config is None:
                view.config = {}

            if view.matches(indicator="thr"):
                view.config["title"] = f"Threshold income per {period} for each decile ({wt_label})"
                subtitle = f"The level of income per person per {period} below which 10%, 20%, 30%, etc. of the population falls. Income here is measured {wt_label}es and benefits. {PPP_ADJUSTMENT_SUBTITLE}"
                view.config["subtitle"] = subtitle
                view.metadata = {"description_short": subtitle}
            elif view.matches(indicator="avg"):
                view.config["title"] = f"Mean income per {period} within each decile ({wt_label})"
                subtitle = f"The mean income per person per {period} within each decile (tenth of the population). Income here is measured {wt_label}es and benefits. {PPP_ADJUSTMENT_SUBTITLE}"
                view.config["subtitle"] = subtitle
                view.metadata = {"description_short": subtitle}
            elif view.matches(indicator="share"):
                view.config["title"] = f"Income share for each decile ({wt_label})"
                subtitle = f"The share of income received by each decile (tenth of the population). Income here is measured {wt_label}es and benefits."
                view.config["subtitle"] = subtitle
                view.metadata = {"description_short": subtitle}

    # Group welfare_type (before vs after tax)
    c.group_views(
        groups=[
            {
                "dimension": "welfare_type",
                "choices": ["dhi", "mi"],
                "choice_new_slug": "before_vs_after",
                "view_config": {
                    "hideRelativeToggle": True,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                    "missingDataStrategy": "hide",
                },
            },
        ],
    )

    # Remove before_vs_after for grouped decile views — too many indicators
    c.drop_views(
        [{"welfare_type": ["before_vs_after"], "decile": ["all", "all_bar", "10_40_50", "10_40_50_bar"]}]
    )

    # Customize before_vs_after views
    for view in c.views:
        if view.dimensions.get("welfare_type") == "before_vs_after" and view.indicators.y:
            first_ind = view.indicators.y[0]
            col_name = first_ind.catalogPath.split("#")[-1] if "#" in first_ind.catalogPath else None

            if col_name and col_name in tb.columns:
                meta = tb[col_name].metadata
                grapher_config = meta.presentation.grapher_config if meta.presentation else {}

                # Extract and modify title
                title = grapher_config.get("title", "")
                title = title.replace("after tax", "before vs. after tax")

                # Extract and modify subtitle (remove welfare type phrase)
                subtitle = grapher_config.get("subtitle", "")
                subtitle = subtitle.replace(" Income here is measured after taxes and benefits.", "")

                # Get description_key and remove the welfare type item
                description_key = list(meta.description_key) if meta.description_key else []
                description_key = [k for k in description_key if "post-tax" not in k and "pre-tax" not in k]

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
                    "missingDataStrategy": "hide",
                }

                # Set metadata
                view.metadata = {
                    "description_short": subtitle,
                    "description_key": description_key,
                }

            # Set display names for each indicator
            for ind in view.indicators.y:
                if "_dhi_" in ind.catalogPath:
                    ind.display = {"name": "After tax"}
                elif "_mi_" in ind.catalogPath:
                    ind.display = {"name": "Before tax"}

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
            # Remove welfare type suffix if present
            for suffix in [", after tax", ", before tax"]:
                if extracted.endswith(suffix):
                    extracted = extracted[: -len(suffix)]
                    break
            return extracted[0].upper() + extracted[1:] if extracted else extracted
    return None

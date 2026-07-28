"""Multidim export for LIS incomes across the distribution."""

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

# Define equivalence_scale
EQUIVALENCE_SCALE = "square root"

PPP_ADJUSTMENT_SUBTITLE = "This data is adjusted for inflation and differences in living costs between countries."

# Override of description_key_welfare_type (luxembourg_income_study.meta.yml line 109) for the grouped
# welfare_type=before_vs_after view. The OLD_* constants mirror the garden text verbatim — the assertion
# in _get_before_vs_after_metadata catches drift in the source.
OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI = "Income is measured after taxes have been paid and government benefits — such as public pensions, unemployment benefits, and social assistance — have been received."
OLD_DESCRIPTION_KEY_WELFARE_TYPE_MI = "Income is measured before taxes have been paid and government benefits — such as public pensions, unemployment benefits, and social assistance — have been received. Private pension income is also included, meaning a retired person’s before-tax income varies depending on whether their country’s pensions system is primarily public, private or a mix."
NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER = "This data is based on income measured both before and after taxes have been paid and government benefits received, which are shown as separate series. Comparing the two gives a sense of the role of redistribution through a country's tax and benefits system."


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("luxembourg_income_study")
    tb = ds.read("incomes", load_data=False)

    # Filter to "square root" equivalence_scale and remove that dimension
    columns_to_keep = []
    for column in tb.drop(columns=["country", "year"]).columns:
        dims = tb[column].metadata.dimensions
        if dims and dims.get("equivalence_scale") == EQUIVALENCE_SCALE:
            columns_to_keep.append(column)
            dims.pop("equivalence_scale")
            # Convert integer decile values to clean strings (e.g. 1 -> "1", not "1.0")
            if "decile" in dims and isinstance(dims["decile"], (int, float)):
                dims["decile"] = str(int(dims["decile"]))
    tb = tb[columns_to_keep]

    # Create collection
    c = paths.create_collection(
        config=config,
        short_name="incomes_lis",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # Group all deciles together
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
                    "hideRelativeToggle": True,
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
                    "chartTypes": ["LineChart", "DiscreteBar"],
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

    # Filter decile views: keep only relevant deciles per indicator
    non_share = [i for i in c.dimension_choices["indicator"] if i != "share"]
    non_thr = [i for i in c.dimension_choices["indicator"] if i != "thr"]
    c.drop_views(
        [
            {"decile": ["2", "3", "4", "6", "7", "8"]},
            {"decile": ["10_40_50"], "indicator": non_share},
            {"decile": ["5", "9"], "indicator": non_thr},
            {"decile": ["p10_p50_p90"], "indicator": non_thr},
        ]
    )

    # Build indicator display names from metadata
    indicator_display_names = _build_indicator_display_names(tb)

    # Customize grouped decile views: sort indicators and set display names
    for view in c.views:
        if view.matches(decile=["all", "p10_p50_p90"]) and view.indicators.y:
            # Sort indicators by decile number
            reverse_order = view.matches(indicator="share")
            view.indicators.y = sorted(view.indicators.y, key=_get_decile_number, reverse=reverse_order)

            # Set sortBy to first indicator in the list
            view.config = view.config or {}
            view.config["sortBy"] = "column"
            view.config["sortColumnSlug"] = view.indicators.y[0].catalogPath

            # Set display names
            for ind in view.indicators.y:
                name = _get_display_name_from_metadata(ind, indicator_display_names)
                if name:
                    ind.display = {"name": name}

    # Group welfare_type (before vs after tax)
    c.group_views(
        groups=[
            {
                "dimension": "welfare_type",
                "choices": ["mi", "dhi"],
                "choice_new_slug": "before_vs_after",
                "view_config": {
                    "hideRelativeToggle": True,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart", "Dumbbell"],
                    "missingDataStrategy": "hide",
                    # Sort the dumbbell (and table) entities by the after-tax value, lowest first
                    "sortBy": "column",
                    "sortColumnSlug": _after_tax_catalog_path,
                    "sortOrder": "asc",
                    "title": "{title}",
                    "subtitle": "{subtitle}",
                    "note": "",
                },
                "view_metadata": {
                    "description_short": "{subtitle}",
                    "description_key": lambda view: _get_before_vs_after_metadata(tb, view)["description_key"],
                },
            },
        ],
        params={
            "title": lambda view: _get_before_vs_after_metadata(tb, view)["title"],
            "subtitle": lambda view: _get_before_vs_after_metadata(tb, view)["subtitle"],
        },
    )

    # Remove before_vs_after for grouped decile views — too many indicators
    c.drop_views(
        [
            {
                "welfare_type": ["before_vs_after"],
                "decile": ["all", "10_40_50", "p10_p50_p90"],
            }
        ]
    )

    # Set display names for before_vs_after views
    for view in c.views:
        if view.dimensions.get("welfare_type") == "before_vs_after" and view.indicators.y:
            for ind in view.indicators.y:
                if "_dhi_" in ind.catalogPath:
                    ind.display = {"name": "After taxes and benefits"}
                elif "_mi_" in ind.catalogPath:
                    ind.display = {"name": "Before taxes and benefits"}

    c.save()


def _get_grouped_decile_title(view):
    """Return title for grouped decile views based on indicator type."""
    period = view.dimensions.get("period")
    wt_label = "after tax" if view.dimensions.get("welfare_type") == "dhi" else "before tax"
    titles = {
        "thr": f"Threshold income per {period} for each decile ({wt_label})",
        "avg": f"Mean income per {period} within each decile ({wt_label})",
        "share": f"Income share for each decile ({wt_label})",
    }
    return titles.get(view.dimensions.get("indicator"), "")


def _get_grouped_decile_subtitle(view):
    """Return subtitle for grouped decile views based on indicator type."""
    period = view.dimensions.get("period")
    wt_label = "after tax" if view.dimensions.get("welfare_type") == "dhi" else "before tax"
    subtitles = {
        "thr": f"The level of income per person per {period} below which 10%, 20%, 30%, etc. of the population falls. Income here is measured {wt_label}es and benefits. {PPP_ADJUSTMENT_SUBTITLE}",
        "avg": f"The mean income per person per {period} within each decile (tenth of the population). Income here is measured {wt_label}es and benefits. {PPP_ADJUSTMENT_SUBTITLE}",
        "share": f"The share of income received by each decile (tenth of the population). Income here is measured {wt_label}es and benefits.",
    }
    return subtitles.get(view.dimensions.get("indicator"), "")


def _get_p10_p50_p90_title(view):
    """Return title for the P10/P50/P90 grouped threshold view."""
    period = view.dimensions.get("period")
    wt_label = "after tax" if view.dimensions.get("welfare_type") == "dhi" else "before tax"
    return f"Threshold income per {period} marking the poorest decile, the median, and the richest decile ({wt_label})"


def _get_p10_p50_p90_subtitle(view):
    """Return subtitle for the P10/P50/P90 grouped threshold view."""
    period = view.dimensions.get("period")
    wt_label = "after tax" if view.dimensions.get("welfare_type") == "dhi" else "before tax"
    return (
        f"The level of income per person per {period} below which 10%, 50% and 90% of the population falls. "
        f"Income here is measured {wt_label}es and benefits. {PPP_ADJUSTMENT_SUBTITLE}"
    )


def _after_tax_catalog_path(view):
    """Return the after-tax (dhi) indicator's catalogPath for a before_vs_after view (used to sort entities by it)."""
    return next((i.catalogPath for i in view.indicators.y if "_dhi_" in i.catalogPath), None)


def _get_before_vs_after_metadata(tb, view):
    """Extract and transform metadata from grapher_config for before_vs_after views.

    Returns a dict with 'title', 'subtitle', and 'description_key'.
    """
    if not view.indicators.y:
        return {"title": "", "subtitle": "", "description_key": []}

    # Build the combined title/subtitle from the before-tax (mi) indicator, so it doesn't
    # depend on the order of indicators in the view (mirrors the WID before_vs_after logic).
    first_ind = next((i for i in view.indicators.y if "_mi_" in i.catalogPath), view.indicators.y[0])
    col_name = first_ind.catalogPath.split("#")[-1] if "#" in first_ind.catalogPath else None

    if col_name and col_name in tb.columns:
        meta = tb[col_name].metadata
        grapher_config = meta.presentation.grapher_config if meta.presentation else {}

        title = _assert_and_replace(
            grapher_config.get("title", ""), "before tax", "before vs. after tax", "grapher_config.title", col_name
        )
        subtitle = _assert_and_replace(
            grapher_config.get("subtitle", ""),
            " Income here is measured before taxes and benefits.",
            "",
            "grapher_config.subtitle",
            col_name,
        )

        description_key = _description_key_bullets(meta)
        old_welfare_keys = {OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI, OLD_DESCRIPTION_KEY_WELFARE_TYPE_MI}
        assert any(b in old_welfare_keys for b in description_key), (
            f"Neither OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI nor _MI found in {col_name}.description_key — garden text changed, update the constants."
        )
        description_key = [NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER if b in old_welfare_keys else b for b in description_key]

        return {"title": title, "subtitle": subtitle, "description_key": description_key}

    return {"title": "", "subtitle": "", "description_key": []}


def _description_key_bullets(meta):
    """Return an indicator's description_key as a list of bullet strings.

    The grapher channel stores description_key as a single markdown string (bullets joined with
    "\\n- "); older builds stored a list. Normalize both to a list so bullets can be swapped.
    """
    dk = meta.description_key if meta else None
    if dk is None:
        return []
    if not isinstance(dk, str):
        return list(dk)
    lines = [line.strip() for line in dk.split("\n") if line.strip()]
    if lines and all(line.startswith("- ") for line in lines):
        return [line[2:].strip() for line in lines]
    return [dk.strip()]


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
            name = extracted[0].upper() + extracted[1:] if extracted else extracted
            # In thr views, annotate the 5th decile as the median.
            if name and col_name.startswith("thr__") and _get_decile_number(ind) == 5:
                name = f"{name} (median)"
            return name
    return None


def _assert_and_replace(text, old, new, field, col_name):
    """Replace `old` with `new` in `text`; assert `old` was present so silent drift in the garden meta surfaces as a clear error."""
    assert old in text, f"'{old}' not found in {col_name}.{field} — garden text changed, update the replacement."
    return text.replace(old, new)

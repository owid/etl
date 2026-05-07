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

# Override of description_key_welfare_type (luxembourg_income_study.meta.yml line 108) for the grouped
# welfare_type=before_vs_after view. The OLD_* constants mirror the garden text verbatim — the assertion
# in _get_before_vs_after_metadata catches drift in the source.
OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI = (
    "Income is measured after taxes have been paid and most government benefits have been received."
)
OLD_DESCRIPTION_KEY_WELFARE_TYPE_MI = (
    "Income is measured before taxes have been paid and most government benefits have been received."
)
NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER_SHARE = "This data is based on income measured both before and after taxes and benefits, which are shown separately. Taxes and benefits typically increase the share going to poorer groups and reduce the share going to richer groups."
NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER_REST = "This data is based on income measured both before and after taxes and benefits, which are shown separately. Taxes and benefits typically raise incomes at the bottom of the distribution and reduce incomes at the top."

# Override of description_key_thr (luxembourg_income_study.meta.yml line 147) for the grouped thr+decile=all view.
OLD_DESCRIPTION_KEY_THR = 'This data shows the income threshold for a given decile — a tenth of the population. The "poorest decile" threshold, for example, is the income level below which the poorest 10% of people in a country fall.'
NEW_DESCRIPTION_KEY_THR_ALL = 'This data shows the income threshold for each decile of the population. The "poorest decile" threshold, for example, is the income level below which the poorest 10% of people in a country fall.'


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

    # Filter decile views: keep only relevant deciles per indicator
    non_share = [i for i in c.dimension_choices["indicator"] if i != "share"]
    non_thr = [i for i in c.dimension_choices["indicator"] if i != "thr"]
    c.drop_views(
        [
            {"decile": ["2", "3", "4", "6", "7", "8"]},
            {"decile": ["10_40_50"], "indicator": non_share},
            {"decile": ["5", "9"], "indicator": non_thr},
        ]
    )

    # Build indicator display names from metadata
    indicator_display_names = _build_indicator_display_names(tb)

    # Customize grouped decile views: sort indicators and set display names
    for view in c.views:
        if view.matches(decile="all") and view.indicators.y:
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

    # description_key_thr's "given decile" wording fits single-decile views; rewrite it for the grouped all-decile view while preserving the indicator's other bullets.
    for view in c.views:
        if view.matches(indicator="thr", decile="all") and view.indicators.y:
            col_name = view.indicators.y[0].catalogPath.split("#")[-1]
            source_description_key = list(tb[col_name].metadata.description_key) if col_name in tb.columns else []
            assert OLD_DESCRIPTION_KEY_THR in source_description_key, (
                f"OLD_DESCRIPTION_KEY_THR not found in {col_name}.description_key — garden text changed, update OLD_DESCRIPTION_KEY_THR/NEW_DESCRIPTION_KEY_THR_ALL."
            )
            view.metadata = view.metadata or {}
            view.metadata["description_key"] = [
                NEW_DESCRIPTION_KEY_THR_ALL if b == OLD_DESCRIPTION_KEY_THR else b for b in source_description_key
            ]

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
                "decile": ["all", "10_40_50"],
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


def _get_before_vs_after_metadata(tb, view):
    """Extract and transform metadata from grapher_config for before_vs_after views.

    Returns a dict with 'title', 'subtitle', and 'description_key'.
    """
    if not view.indicators.y:
        return {"title": "", "subtitle": "", "description_key": []}

    first_ind = view.indicators.y[0]
    col_name = first_ind.catalogPath.split("#")[-1] if "#" in first_ind.catalogPath else None

    if col_name and col_name in tb.columns:
        meta = tb[col_name].metadata
        grapher_config = meta.presentation.grapher_config if meta.presentation else {}

        title = _assert_and_replace(
            grapher_config.get("title", ""), "after tax", "before vs. after tax", "grapher_config.title", col_name
        )
        subtitle = _assert_and_replace(
            grapher_config.get("subtitle", ""),
            " Income here is measured after taxes and benefits.",
            "",
            "grapher_config.subtitle",
            col_name,
        )

        description_key = list(meta.description_key) if meta.description_key else []
        old_welfare_keys = {OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI, OLD_DESCRIPTION_KEY_WELFARE_TYPE_MI}
        assert any(b in old_welfare_keys for b in description_key), (
            f"Neither OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI nor _MI found in {col_name}.description_key — garden text changed, update the constants."
        )
        new_text = (
            NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER_SHARE
            if view.dimensions.get("indicator") == "share"
            else NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER_REST
        )
        description_key = [new_text if b in old_welfare_keys else b for b in description_key]

        return {"title": title, "subtitle": subtitle, "description_key": description_key}

    return {"title": "", "subtitle": "", "description_key": []}


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

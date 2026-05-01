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

# Define if data is extrapolated or not
EXTRAPOLATED = "no"

# Override of description_key_welfare_type (world_inequality_database.meta.yml line 123) for the grouped
# welfare_type=before_vs_after views. The OLD_* constants mirror the garden text verbatim — the assertion
# in _replace_welfare_type_bullet catches drift in the source.
OLD_DESCRIPTION_KEY_WELFARE_TYPE_BEFORE_TAX = "Income is measured before taxes have been paid and most government benefits have been received. The exception is pensions and other social insurance, such as unemployment insurance. Contributions to social insurance are deducted, and the corresponding benefits are added back and counted as income."
OLD_DESCRIPTION_KEY_WELFARE_TYPE_AFTER_TAX = "Income is measured after taxes have been paid and most government benefits have been received. Not just cash benefits like social assistance, but also public services like health and education, and collective spending, such as defense and infrastructure. This is a broader concept of income than used by most other sources."
NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER = "This data is based on income measured both before and after taxes and benefits, which are shown separately. Taxes and benefits typically increase the share going to poorer groups and reduce the share going to richer groups."

# Sourced from the after_tax indicator's description_key. The before_vs_after view inherits from the
# before_tax indicator, so this bullet is otherwise lost; we re-attach it as the last bullet.
DESCRIPTION_KEY_AFTER_TAX_AVAILABILITY = "Data on income after tax and benefits is less widely available than that before tax. Where it is missing, distributions are constructed from the more widely available pre-tax data, combined with data on tax revenue and government expenditure. This method is described in more detail in this [technical note](https://wid.world/document/preliminary-estimates-of-global-posttax-income-distributions-world-inequality-lab-technical-note-2023-02/)."


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
        if dims and dims.get("extrapolated") == EXTRAPOLATED:
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
                    "chartTypes": ["StackedArea", "StackedDiscreteBar"],
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
    keep_quantiles = {
        "Richest 0.1%",
        "Richest 1%",
        "10",
        "10_40_50",
        "all",
    }
    c.drop_views({"quantile": [q for q in c.dimension_choices["quantile"] if q not in keep_quantiles]})

    # Build mapping of catalogPath to display name from table metadata
    indicator_display_names = _build_indicator_display_names(tb)

    # Customize grouped quantile views: sort indicators and set display names
    for view in c.views:
        quantile = view.dimensions.get("quantile")
        if quantile == "all" and view.indicators.y:
            # Sort indicators by decile number (richest to poorest)
            view.indicators.y = sorted(view.indicators.y, key=_get_decile_number, reverse=True)

            # Set sortBy to first indicator in the list
            view.config = view.config or {}
            view.config["sortBy"] = "column"
            view.config["sortColumnSlug"] = view.indicators.y[0].catalogPath

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
    c.drop_views(
        {
            "welfare_type": ["before_vs_after"],
            "quantile": ["10_40_50", "all"],
        }
    )

    # Set display names for before_vs_after views
    for view in c.views:
        if view.dimensions.get("welfare_type") == "before_vs_after" and view.indicators.y:
            for ind in view.indicators.y:
                if "before_tax" in ind.catalogPath:
                    ind.display = {"name": "Before taxes and benefits"}
                elif "after_tax" in ind.catalogPath:
                    ind.display = {"name": "After taxes and benefits"}

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

        title = _assert_and_replace(
            grapher_config.get("title", ""),
            "before tax",
            "before vs. after tax",
            "grapher_config.title",
            col_name,
        )
        subtitle = _assert_and_replace(
            grapher_config.get("subtitle", ""),
            " Income here is measured before taxes and benefits.",
            "",
            "grapher_config.subtitle",
            col_name,
        )
        description_short = _assert_and_replace(
            meta.description_short or "",
            " Income here is measured before taxes and benefits.",
            "",
            "description_short",
            col_name,
        )

        description_key = list(meta.description_key) if meta.description_key else []
        description_key = _replace_welfare_type_bullet(description_key, col_name)
        description_key = _append_after_tax_availability_bullet(description_key, tb, view)

        return {
            "title": title,
            "subtitle": subtitle,
            "description_short": description_short,
            "description_key": description_key,
        }

    return {"title": "", "subtitle": "", "description_short": "", "description_key": []}


def _replace_welfare_type_bullet(description_key, col_name):
    """Replace the welfare_type bullet (before/after tax variants) with the combined before_vs_after wording."""
    old_welfare_keys = {OLD_DESCRIPTION_KEY_WELFARE_TYPE_BEFORE_TAX, OLD_DESCRIPTION_KEY_WELFARE_TYPE_AFTER_TAX}
    assert any(b in old_welfare_keys for b in description_key), (
        f"Neither OLD_DESCRIPTION_KEY_WELFARE_TYPE_BEFORE_TAX nor _AFTER_TAX found in {col_name}.description_key — garden text changed, update the constants."
    )
    return [NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER if b in old_welfare_keys else b for b in description_key]


def _append_after_tax_availability_bullet(description_key, tb, view):
    """Append the after_tax-only availability caveat (sourced from the after_tax indicator) as the last bullet."""
    if DESCRIPTION_KEY_AFTER_TAX_AVAILABILITY in description_key:
        return description_key
    after_tax_ind = next((i for i in view.indicators.y if "after_tax" in i.catalogPath), None)
    if after_tax_ind:
        after_tax_col = after_tax_ind.catalogPath.split("#")[-1]
        after_tax_description_key = (
            list(tb[after_tax_col].metadata.description_key or []) if after_tax_col in tb.columns else []
        )
        assert DESCRIPTION_KEY_AFTER_TAX_AVAILABILITY in after_tax_description_key, (
            f"DESCRIPTION_KEY_AFTER_TAX_AVAILABILITY not found in {after_tax_col}.description_key — garden text changed, update the constant."
        )
        description_key.append(DESCRIPTION_KEY_AFTER_TAX_AVAILABILITY)
    return description_key


def _assert_and_replace(text, old, new, field, col_name):
    """Replace `old` with `new` in `text`; assert `old` was present so silent drift in the garden meta surfaces as a clear error."""
    assert old in text, f"'{old}' not found in {col_name}.{field} — garden text changed, update the replacement."
    return text.replace(old, new)


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

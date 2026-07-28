"""Multidim export for LIS Gini coefficient."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Define indicators to use
INDICATORS = ["gini"]

# Define dimensions for main views
DIMENSIONS_CONFIG = {
    "welfare_type": "*",
    "equivalence_scale": ["square root"],
}

# Override of description_key_welfare_type (luxembourg_income_study.meta.yml line 109) for the grouped
# welfare_type=before_vs_after view. The OLD_* constants mirror the garden text verbatim — the assertion
# in _get_before_vs_after_metadata catches drift in the source.
OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI = "Income is measured after taxes have been paid and government benefits — such as public pensions, unemployment benefits, and social assistance — have been received."
OLD_DESCRIPTION_KEY_WELFARE_TYPE_MI = "Income is measured before taxes have been paid and government benefits — such as public pensions, unemployment benefits, and social assistance — have been received. Private pension income is also included, meaning a retired person’s before-tax income varies depending on whether their country’s pensions system is primarily public, private or a mix."
NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER = "This data is based on income measured both before and after taxes have been paid and government benefits received, which are shown as separate series. Comparing the two gives a sense of the role of redistribution through a country's tax and benefits system."


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("luxembourg_income_study")
    tb = ds.read("inequality", load_data=False)

    c = paths.create_collection(
        config=config,
        short_name="gini_lis",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # Group welfare_type (before vs after tax) as line chart
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

    # Set display names for before_vs_after views
    for view in c.views:
        if view.dimensions.get("welfare_type") == "before_vs_after" and view.indicators.y:
            for ind in view.indicators.y:
                if "_dhi_" in ind.catalogPath:
                    ind.display = {"name": "After taxes and benefits"}
                elif "_mi_" in ind.catalogPath:
                    ind.display = {"name": "Before taxes and benefits"}

    c.save()


def _after_tax_catalog_path(view):
    """Return the after-tax (dhi) indicator's catalogPath for a before_vs_after view (used to sort entities by it)."""
    return next((i.catalogPath for i in view.indicators.y if "_dhi_" in i.catalogPath), None)


def _get_before_vs_after_metadata(tb, view):
    """Extract and transform metadata from grapher_config for before_vs_after views."""
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
            grapher_config.get("title", "Gini coefficient"),
            "before tax",
            "before vs. after tax",
            "grapher_config.title",
            col_name,
        )
        subtitle = _assert_and_replace(
            grapher_config.get("subtitle", ""),
            " Inequality is measured here in terms of income before taxes and benefits.",
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


def _assert_and_replace(text, old, new, field, col_name):
    """Replace `old` with `new` in `text`; assert `old` was present so silent drift in the garden meta surfaces as a clear error."""
    assert old in text, f"'{old}' not found in {col_name}.{field} — garden text changed, update the replacement."
    return text.replace(old, new)


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

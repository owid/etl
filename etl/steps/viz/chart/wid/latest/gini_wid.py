"""Multidim export for WID Gini coefficient."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Define indicators to use
INDICATORS = ["gini"]

# Define dimensions for main views
DIMENSIONS_CONFIG = {
    "welfare_type": ["before tax", "after tax"],
    "extrapolated": ["no"],
}

# Override of description_key_welfare_type (world_inequality_database.meta.yml line 144) for the grouped
# welfare_type=before_vs_after view. The OLD_* constants mirror the garden text verbatim — the assertion
# in _get_before_vs_after_metadata catches drift in the source.
OLD_DESCRIPTION_KEY_WELFARE_TYPE_BEFORE_TAX = "Before tax income is measured before taxes have been paid and most government benefits have been received. The exception is pensions and other social insurance benefits, such as unemployment insurance — contributions to these are deducted, and the corresponding benefits are added back as income. [Read more](#dod:wid-income-definition)."
OLD_DESCRIPTION_KEY_WELFARE_TYPE_AFTER_TAX = "After tax income is measured after taxes have been paid and government benefits have been received. This includes not only cash benefits like social assistance, but also the value of public services like hospitals and schools, and collective spending, such as defense and infrastructure. This is a broader concept of income than used by survey-based data sources."
NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER = "This data is based on income measured both before and after taxes have been paid and government benefits received, which are shown as separate series. Comparing the two gives a sense of the role of redistribution through a country's tax and benefits system."


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("world_inequality_database")
    tb = ds.read("inequality", load_data=False)

    c = paths.create_collection(
        config=config,
        short_name="gini_wid",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # Group welfare_type (before vs after tax) as line chart
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
                    "chartTypes": ["LineChart", "Dumbbell"],
                    "missingDataStrategy": "hide",
                    # Sort the dumbbell (and table) entities by the after-tax value, lowest first
                    "sortBy": "column",
                    "sortColumnSlug": _after_tax_catalog_path,
                    "sortOrder": "asc",
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
                if "before_tax" in ind.catalogPath:
                    ind.display = {"name": "Before taxes and benefits"}
                elif "after_tax" in ind.catalogPath:
                    ind.display = {"name": "After taxes and benefits"}

    c.save()


def _after_tax_catalog_path(view):
    """Return the after-tax indicator's catalogPath for a before_vs_after view (used to sort entities by it)."""
    return next((i.catalogPath for i in view.indicators.y if "after_tax" in i.catalogPath), None)


def _get_before_vs_after_metadata(tb, view):
    """Extract and transform metadata from grapher_config for before_vs_after views."""
    if not view.indicators.y:
        return {"title": "", "subtitle": "", "description_key": []}

    first_ind = view.indicators.y[0]
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
        old_welfare_keys = {OLD_DESCRIPTION_KEY_WELFARE_TYPE_BEFORE_TAX, OLD_DESCRIPTION_KEY_WELFARE_TYPE_AFTER_TAX}
        assert any(b in old_welfare_keys for b in description_key), (
            f"Neither OLD_DESCRIPTION_KEY_WELFARE_TYPE_BEFORE_TAX nor _AFTER_TAX found in {col_name}.description_key — garden text changed, update the constants."
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

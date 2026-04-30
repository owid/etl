"""Multidim export for WID Palma ratio."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Define indicators to use
INDICATORS = ["palma_ratio"]

# Define dimensions for main views
DIMENSIONS_CONFIG = {
    "welfare_type": ["before tax", "after tax"],
    "extrapolated": ["no"],
}

# Override of description_key_welfare_type (world_inequality_database.meta.yml line 123) for the grouped
# welfare_type=before_vs_after view. The OLD_* constants mirror the garden text verbatim — the assertion
# in _get_before_vs_after_metadata catches drift in the source.
OLD_DESCRIPTION_KEY_WELFARE_TYPE_BEFORE_TAX = "Income is measured before taxes have been paid and most government benefits have been received. The exception is pensions and other social insurance, such as unemployment insurance. Contributions to social insurance are deducted, and the corresponding benefits are added back and counted as income."
OLD_DESCRIPTION_KEY_WELFARE_TYPE_AFTER_TAX = "Income is measured after taxes have been paid and most government benefits have been received. Not just cash benefits like social assistance, but also public services like health and education, and collective spending, such as defense and infrastructure. This is a broader concept of income than used by most other sources."
NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER = "This data is based on income measured both before and after taxes and benefits, which are shown separately. In most countries, inequality is lower after taxes and benefits than before, but the extent varies widely."

# Sourced from the after_tax indicator's description_key. The before_vs_after view inherits from the
# before_tax indicator, so this bullet is otherwise lost; we re-attach it as the last bullet.
DESCRIPTION_KEY_AFTER_TAX_AVAILABILITY = "Data on income after tax and benefits is less widely available than that before tax. Where it is missing, distributions are constructed from the more widely available pre-tax data, combined with data on tax revenue and government expenditure. This method is described in more detail in this [technical note](https://wid.world/document/preliminary-estimates-of-global-posttax-income-distributions-world-inequality-lab-technical-note-2023-02/)."


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("world_inequality_database")
    tb = ds.read("inequality", load_data=False)

    c = paths.create_collection(
        config=config,
        short_name="palma_ratio_wid",
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
                    "chartTypes": ["LineChart"],
                    "missingDataStrategy": "hide",
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
            grapher_config.get("title", "Palma ratio"),
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

        description_key = list(meta.description_key) if meta.description_key else []
        old_welfare_keys = {OLD_DESCRIPTION_KEY_WELFARE_TYPE_BEFORE_TAX, OLD_DESCRIPTION_KEY_WELFARE_TYPE_AFTER_TAX}
        assert any(b in old_welfare_keys for b in description_key), (
            f"Neither OLD_DESCRIPTION_KEY_WELFARE_TYPE_BEFORE_TAX nor _AFTER_TAX found in {col_name}.description_key — garden text changed, update the constants."
        )
        description_key = [NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER if b in old_welfare_keys else b for b in description_key]

        # The before_vs_after view inherits from the before_tax indicator; pull the after_tax-only
        # availability caveat from the matching after_tax indicator and append it.
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

        return {"title": title, "subtitle": subtitle, "description_key": description_key}

    return {"title": "", "subtitle": "", "description_key": []}


def _assert_and_replace(text, old, new, field, col_name):
    """Replace `old` with `new` in `text`; assert `old` was present so silent drift in the garden meta surfaces as a clear error."""
    assert old in text, f"'{old}' not found in {col_name}.{field} — garden text changed, update the replacement."
    return text.replace(old, new)

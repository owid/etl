"""Multidim export for LIS relative poverty indicators."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Define indicators to use
INDICATORS = ["headcount_ratio", "headcount"]

# Define dimensions for main views
DIMENSIONS_CONFIG = {
    "poverty_line": ["40% of the median", "50% of the median", "60% of the median"],
    "welfare_type": "*",
    "equivalence_scale": ["square root"],
}

# Override of description_key_welfare_type (luxembourg_income_study.meta.yml line 108) for the grouped
# welfare_type=before_vs_after view. The OLD_* constants mirror the garden text verbatim — the assertion
# in _get_before_vs_after_metadata catches drift in the source.
OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI = (
    "Income is measured after taxes have been paid and most government benefits have been received."
)
OLD_DESCRIPTION_KEY_WELFARE_TYPE_MI = (
    "Income is measured before taxes have been paid and most government benefits have been received."
)
NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER = "This data is based on income measured both before and after taxes and benefits, which are shown separately. In most countries, relative poverty is lower after taxes and benefits than before, but the extent varies widely."


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("luxembourg_income_study")
    tb = ds.read("poverty", load_data=False)

    c = paths.create_collection(
        config=config,
        short_name="relative_poverty_lis",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # Group welfare_type (before vs after tax) as line chart
    c.group_views(
        groups=[
            {
                "dimension": "welfare_type",
                "choices": ["dhi", "mi"],
                "choice_new_slug": "before_vs_after",
                "view_config": {
                    "title": "{title}",
                    "subtitle": "{subtitle}",
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
                if "_dhi_" in ind.catalogPath:
                    ind.display = {"name": "After taxes and benefits"}
                elif "_mi_" in ind.catalogPath:
                    ind.display = {"name": "Before taxes and benefits"}

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

        title = grapher_config.get("title", "")
        title = title.replace("after tax", "before vs. after tax")

        subtitle = grapher_config.get("subtitle", "")
        subtitle = subtitle.replace(" Income here is measured after taxes and benefits.", "")

        description_key = list(meta.description_key) if meta.description_key else []
        old_welfare_keys = {OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI, OLD_DESCRIPTION_KEY_WELFARE_TYPE_MI}
        assert any(b in old_welfare_keys for b in description_key), (
            f"Neither OLD_DESCRIPTION_KEY_WELFARE_TYPE_DHI nor _MI found in {col_name}.description_key — garden text changed, update the constants."
        )
        description_key = [NEW_DESCRIPTION_KEY_BEFORE_VS_AFTER if b in old_welfare_keys else b for b in description_key]

        return {"title": title, "subtitle": subtitle, "description_key": description_key}

    return {"title": "", "subtitle": "", "description_key": []}

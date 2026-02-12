"""Multidim export for WID Gini coefficient."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Define indicators to use
INDICATORS = ["gini"]

# Define dimensions for main views
DIMENSIONS_CONFIG = {
    "welfare_type": ["before tax", "after tax"],
}

# Define if data is extrapolated or not
EXTRAPOLATED = "no"


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("world_inequality_database")
    tb = ds.read("inequality", load_data=False)

    # Filter columns to only keep extrapolated=no, then remove that dimension from metadata.
    columns_to_keep = []
    for column in tb.drop(columns=["country", "year"]).columns:
        dims = tb[column].metadata.dimensions
        if dims and dims.get("extrapolated") == EXTRAPOLATED:
            columns_to_keep.append(column)
            dims.pop("extrapolated")
    tb = tb[columns_to_keep]

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
                    ind.display = {"name": "Before tax"}
                elif "after_tax" in ind.catalogPath:
                    ind.display = {"name": "After tax"}

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

        title = grapher_config.get("title", "Gini coefficient")
        title = title.replace("before tax", "before vs. after tax")

        subtitle = grapher_config.get("subtitle", "")
        subtitle = subtitle.replace(" Inequality is measured here in terms of income before taxes and benefits.", "")

        description_key = list(meta.description_key) if meta.description_key else []
        if description_key:
            description_key = description_key[1:]

        return {"title": title, "subtitle": subtitle, "description_key": description_key}

    return {"title": "", "subtitle": "", "description_key": []}

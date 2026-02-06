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

    # Customize grouped welfare_type views (before_vs_after)
    for view in c.views:
        if view.dimensions.get("welfare_type") == "before_vs_after" and view.indicators.y:
            # Get metadata from first indicator
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

                # Get description_key and remove second element (welfare type description)
                description_key = list(meta.description_key) if meta.description_key else []
                if len(description_key) > 1:
                    description_key.pop(1)

                # Set config
                view.config = {
                    "title": title,
                    "subtitle": subtitle,
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

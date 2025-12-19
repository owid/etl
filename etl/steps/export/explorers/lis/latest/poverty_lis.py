"""Load a meadow dataset and create a garden dataset."""

from etl.collection.model.dimension import DimensionPresentation
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicators to use
INDICATOR_NAMES = [
    "headcount_ratio",
    "headcount",
    "total_shortfall",
    "avg_shortfall",
    "income_gap_ratio",
    "poverty_gap_index",
]

DIMENSIONS_CONFIG = {
    "poverty_line": [
        "1",
        "2",
        "5",
        "10",
        "20",
        "30",
        "40",
        "40% of the median",
        "50% of the median",
        "60% of the median",
    ],
    "welfare_type": ["dhi", "mi"],
    "equivalence_scale": ["per capita", "square root"],
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("luxembourg_income_study")
    tb = ds.read("poverty", load_data=False)

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="lis_poverty",
        tb=tb,
        indicator_names=INDICATOR_NAMES,
        # dimensions=DIMENSIONS_CONFIG,
        explorer=True,
    )

    # Make the equivalence_scale dimension a checkbox
    for dimension in c.dimensions:
        if dimension.slug == "equivalence_scale":
            dimension.presentation = DimensionPresentation(type="checkbox", choice_slug_true="square root")

    # Add After tax vs. Before tax dimension
    c.group_views(
        groups=[
            {
                "dimension": "welfare_type",
                "choice_new_slug": "after_vs_before_tax",
                "view_config": {
                    "hideRelativeToggle": "false",
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": "false",
                    "type": "LineChart",
                },
            },
            {
                "dimension": "poverty_line",
                "choices": ["1", "2", "5", "10", "20", "30", "40"],
                "choice_new_slug": "multiple_lines",
                "view_config": {
                    "hideRelativeToggle": "false",
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": "false",
                    "type": "LineChart",
                },
            },
        ]
    )

    #
    # (optional) Edit views
    #
    for view in c.views:
        # Initialize config if it's None
        if view.config is None:
            view.config = {}

        if view.dimensions["welfare_type"] in ["dhi", "mi"]:
            # Set tab as map
            view.config["tab"] = "map"

        if view.dimensions["welfare_type"] == "after_vs_before_tax":
            # Generate title from first indicator's display name
            # Get the catalog path of the first indicator
            if view.indicators.y:
                first_indicator_path = view.indicators.y[0].catalogPath
                # Extract the column name from the catalog path
                indicator_col = first_indicator_path.split("#")[-1]

                # Get the title from the table metadata
                if indicator_col in tb.columns:
                    col_meta = tb[indicator_col].metadata
                    if col_meta.presentation and col_meta.presentation.grapher_config:
                        # Set title
                        if col_meta.presentation.grapher_config.get("title"):
                            view.config["title"] = col_meta.presentation.grapher_config["title"]

                            # Remove (before tax) or (after tax) from title if present
                            view.config["title"] = (
                                view.config["title"]
                                .replace(" (before tax)", " (after vs. before tax)")
                                .replace(" (after tax)", " (after vs. before tax)")
                            )

                        if col_meta.presentation.grapher_config.get("subtitle"):
                            view.config["subtitle"] = col_meta.presentation.grapher_config["subtitle"]

                            # Remove welfare type info from subtitle
                            view.config["subtitle"] = (
                                view.config["subtitle"]
                                .replace(" Income here is measured after taxes and benefits.", "")
                                .replace(" Income here is measured before taxes and benefits.", "")
                            )

        # Set default view
        if (
            view.dimensions["indicator"] == "headcount_ratio"
            and view.dimensions["poverty_line"] == "30"
            and view.dimensions["welfare_type"] == "dhi"
            and view.dimensions["equivalence_scale"] == "per capita"
        ):
            view.config["defaultView"] = True

    #
    # Save garden dataset.
    #
    c.save()

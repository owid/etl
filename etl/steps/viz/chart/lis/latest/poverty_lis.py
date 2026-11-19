"""Poverty multidim from the Luxembourg Income Study, shown as a data explorer (replaces the poverty-lis explorer)."""

from copy import deepcopy

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

# Define texts to modify
BEFORE_TAX_TITLE = "(before tax)"
AFTER_TAX_TITLE = "(after tax)"
AFTER_VS_BEFORE_TAX_TITLE = "(after vs. before tax)"
BEFORE_TAX_SUBTITLE = "Income here is measured before taxes and benefits."
AFTER_TAX_SUBTITLE = "Income here is measured after taxes and benefits."

EQUIVALENCE_SCALE_NOTE = "Income has been [equivalized](#dod:equivalization)."
EQUIVALENCE_SCALE_SUBTITLE = "Income has been [equivalized](#dod:equivalization) – adjusted to account for the fact that people in the same household can share costs like rent and heating."

# Settings every view gets, grouped ones included (they are created after `common_views` would apply).
# The explorer's text block below the chart only pointed readers to the related article.
ALL_VIEWS_CONFIG = {
    "originUrl": "https://ourworldindata.org/poverty",
    "relatedQuestions": [
        {
            "url": "https://ourworldindata.org/owid-data-collection-inequality-and-poverty",
            "text": "OWID Data Collection: Inequality and Poverty",
        }
    ],
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default chart config
    config = paths.load_config()

    # Load grapher dataset.
    ds = paths.load_dataset("luxembourg_income_study")
    tb = ds.read("poverty", load_data=False)

    #
    # Create the multidim, one view per indicator column
    #
    c = paths.create_chart(
        config=config,
        short_name="poverty_lis",
        tb=tb,
        indicator_names=INDICATOR_NAMES,
    )

    # Add After tax vs. Before tax dimension
    c.group_views(
        groups=[
            {
                "dimension": "welfare_type",
                "choice_new_slug": "after_vs_before_tax",
                "view_config": {
                    "hideRelativeToggle": False,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "chartTypes": ["LineChart"],
                },
            },
            {
                "dimension": "poverty_line",
                "choices": ["1", "3", "4.20", "8.30", "10", "20", "30", "40"],
                "choice_new_slug": "multiple_lines",
                "view_config": {
                    "hideRelativeToggle": False,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "chartTypes": ["LineChart"],
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
        view.config.update(deepcopy(ALL_VIEWS_CONFIG))

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
                                .replace(f" {BEFORE_TAX_TITLE}", f" {AFTER_VS_BEFORE_TAX_TITLE}")
                                .replace(f" {AFTER_TAX_TITLE}", f" {AFTER_VS_BEFORE_TAX_TITLE}")
                            )

                        if col_meta.presentation.grapher_config.get("subtitle"):
                            view.config["subtitle"] = col_meta.presentation.grapher_config["subtitle"]

                            # Remove welfare type info from subtitle
                            view.config["subtitle"] = (
                                view.config["subtitle"]
                                .replace(f" {AFTER_TAX_SUBTITLE}", "")
                                .replace(f" {BEFORE_TAX_SUBTITLE}", "")
                            )

        # Add equivalence scale subtitle when equivalence_scale is "square root"
        if view.dimensions.get("equivalence_scale") == "square root":
            # Check if subtitle was already set by previous block (e.g., after_vs_before_tax)
            existing_subtitle = view.config.get("subtitle", "")

            # If not already set, get it from indicator metadata
            if not existing_subtitle:
                if view.indicators.y:
                    first_indicator_path = view.indicators.y[0].catalogPath
                    indicator_col = first_indicator_path.split("#")[-1]

                    if indicator_col in tb.columns:
                        col_meta = tb[indicator_col].metadata
                        if col_meta.presentation and col_meta.presentation.grapher_config:
                            existing_subtitle = col_meta.presentation.grapher_config.get("subtitle", "")

            # Get note from indicator metadata
            if view.indicators.y:
                first_indicator_path = view.indicators.y[0].catalogPath
                indicator_col = first_indicator_path.split("#")[-1]

                if indicator_col in tb.columns:
                    col_meta = tb[indicator_col].metadata
                    if col_meta.presentation and col_meta.presentation.grapher_config:
                        existing_note = col_meta.presentation.grapher_config.get("note", "")

                        # Remove EQUIVALENCE_SCALE_NOTE if present
                        new_note = existing_note.replace(f"{EQUIVALENCE_SCALE_NOTE}", "").strip()

                        # NOTE: an empty note isn't set, so the view keeps the indicator's note, as the explorer
                        # did (an empty explorer note falls back to it).
                        if new_note:
                            view.config["note"] = new_note

            # Add EQUIVALENCE_SCALE_SUBTITLE at the end
            if existing_subtitle:
                view.config["subtitle"] = f"{existing_subtitle} {EQUIVALENCE_SCALE_SUBTITLE}"
            else:
                view.config["subtitle"] = EQUIVALENCE_SCALE_SUBTITLE

    # The default view is set in the config (`default_dimensions`).
    c.save()

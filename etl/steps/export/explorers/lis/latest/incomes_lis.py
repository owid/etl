"""Load a meadow dataset and create a garden dataset."""

from etl.collection.model.dimension import DimensionPresentation
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicators to use
INDICATOR_NAMES = [
    "mean",
    "median",
    "avg",
    "thr",
    "share",
]

# Define texts to modify
BEFORE_TAX_TITLE = "(before tax)"
AFTER_TAX_TITLE = "(after tax)"
AFTER_VS_BEFORE_TAX_TITLE = "(after vs. before tax)"
BEFORE_TAX_SUBTITLE = "Income here is measured before taxes and benefits."
AFTER_TAX_SUBTITLE = "Income here is measured after taxes and benefits."
BEFORE_TAX_INEQUALITY_SUBTITLE = "Inequality is measured here in terms of income before taxes and benefits."
AFTER_TAX_INEQUALITY_SUBTITLE = "Inequality is measured here in terms of income after taxes and benefits."

EQUIVALENCE_SCALE_NOTE = "Income has been [equivalized](#dod:equivalization)."
EQUIVALENCE_SCALE_SUBTITLE = "Income has been [equivalized](#dod:equivalization) – adjusted to account for the fact that people in the same household can share costs like rent and heating."


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("luxembourg_income_study")
    tb = ds.read("incomes", load_data=False)

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="incomes-across-distribution-lis",
        tb=tb,
        indicator_names=INDICATOR_NAMES,
        # dimensions=DIMENSIONS_CONFIG,
        explorer=True,
    )

    # Make the equivalence_scale dimension a checkbox
    for dimension in c.dimensions:
        if dimension.slug == "equivalence_scale":
            dimension.presentation = DimensionPresentation(type="checkbox", choice_slug_true="square root")

        if dimension.slug == "period":
            dimension.presentation = DimensionPresentation(type="radio")

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
                "dimension": "decile",
                "choice_new_slug": "all",
                "choices": ["1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0", "10.0"],
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
                                .replace(f" {BEFORE_TAX_INEQUALITY_SUBTITLE}", "")
                                .replace(f" {AFTER_TAX_INEQUALITY_SUBTITLE}", "")
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

                        # NOTE: I can't override the note if new_note is empty, because it falls back to the default indicator note.
                        # Remove EQUIVALENCE_SCALE_NOTE if present
                        new_note = existing_note.replace(f"{EQUIVALENCE_SCALE_NOTE}", "").strip()

                        # Set note in view config
                        view.config["note"] = new_note

            # Add EQUIVALENCE_SCALE_SUBTITLE at the end
            if existing_subtitle:
                view.config["subtitle"] = f"{existing_subtitle} {EQUIVALENCE_SCALE_SUBTITLE}"
            else:
                view.config["subtitle"] = EQUIVALENCE_SCALE_SUBTITLE

        # Modify display names for "all" decile views
        if view.dimensions["decile"] == "all":
            # Modify display names for all y indicators
            if view.indicators.y:
                for indicator in view.indicators.y:
                    # Get the catalog path to find the original indicator
                    indicator_path = indicator.catalogPath
                    indicator_col = indicator_path.split("#")[-1]

                    # Get the original display name from table metadata
                    if indicator_col in tb.columns:
                        col_meta = tb[indicator_col].metadata
                        if col_meta.display and col_meta.display.get("name"):
                            original_name = col_meta.display["name"]

                            if original_name:
                                # Remove "Average ", "Threshold ", "Share ", and tax type labels
                                new_name = (
                                    original_name.replace("Average ", "")
                                    .replace("Threshold ", "")
                                    .replace("Share ", "")
                                    .replace(f" {BEFORE_TAX_TITLE}", "")
                                    .replace(f" {AFTER_TAX_TITLE}", "")
                                )

                                # Remove first occurrence of "(" and ")"
                                new_name = new_name.replace("(", "", 1).replace(")", "", 1)

                                # Set the display name for this indicator
                                if not indicator.display:
                                    indicator.display = {}
                                indicator.display["name"] = new_name

        # Modify display names for "after_vs_before_tax" views
        if view.dimensions["welfare_type"] == "after_vs_before_tax":
            # Modify display names for all y indicators
            if view.indicators.y:
                for indicator in view.indicators.y:
                    # Get the catalog path to find the original indicator
                    indicator_path = indicator.catalogPath
                    indicator_col = indicator_path.split("#")[-1]

                    # Get the original display name from table metadata
                    if indicator_col in tb.columns:
                        col_meta = tb[indicator_col].metadata
                        if col_meta.display and col_meta.display.get("name"):
                            original_name = col_meta.display["name"]

                            if original_name:
                                # Remove "Average ", "Threshold ", "Share " but keep tax type labels
                                new_name = (
                                    original_name.replace("Average ", "")
                                    .replace("Threshold ", "")
                                    .replace("Share ", "")
                                )

                                # Remove first occurrence of "(" and ")"
                                new_name = new_name.replace("(", "", 1).replace(")", "", 1)

                                # Set the display name for this indicator
                                if not indicator.display:
                                    indicator.display = {}
                                indicator.display["name"] = new_name

        # Set default view
        if (
            view.dimensions["indicator"] == "thr"
            and view.dimensions["decile"] == "9.0"
            and view.dimensions["welfare_type"] == "dhi"
            and view.dimensions["equivalence_scale"] == "per capita"
        ):
            view.config["defaultView"] = True

    #
    # Save garden dataset.
    #
    c.save()

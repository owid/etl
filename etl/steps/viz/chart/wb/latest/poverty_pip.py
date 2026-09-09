from etl.collection import filter_columns_by_dimension_choices
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP year
PPP_YEAR = 2021

# Define indicators to use
INDICATORS = [
    "headcount_ratio",
    "headcount",
]

# NOTE: Update lines when prices change
DIMENSIONS_CONFIG = {
    "poverty_line": ["100", "300", "420", "830", "1000", "2000", "3000", "4000"],
    "table": ["Income or consumption consolidated", "Income with spells", "Consumption with spells"],
    "survey_comparability": "*",
}


# etlr multidim
def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("world_bank_pip")
    tb = ds.read("poverty", load_data=False)

    # Keep only indicators for a specific PPP year, and drop that dimension.
    tb = filter_columns_by_dimension_choices(tb, {"ppp_version": PPP_YEAR})

    # Remove dimensions that are not needed.
    for column in tb.columns:
        dimensions = tb[column].metadata.dimensions
        if dimensions:
            for dimension in ["welfare_type"]:
                dimensions.pop(dimension, None)

    # Get all survey_comparability values except "No spells" for spell views
    survey_comp_values = set()
    for col in tb.columns:
        if tb[col].metadata.dimensions and "survey_comparability" in tb[col].metadata.dimensions:
            survey_comp_values.add(tb[col].metadata.dimensions["survey_comparability"])
    survey_comp_spells = [v for v in survey_comp_values if v != "No spells"]

    # Create mdim
    c = paths.create_collection(
        config=config,
        short_name="poverty_pip",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # First, group survey_comparability (this must happen first)
    c.group_views(
        groups=[
            {
                "dimension": "survey_comparability",
                "choices": survey_comp_spells,
                "choice_new_slug": "Spells",
                "replace": True,
                "view_config": {
                    "hideRelativeToggle": False,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                },
            },
        ],
    )

    # Then, group the table dimension
    c.group_views(
        groups=[
            {
                "dimension": "table",
                "choices": ["Income with spells", "Consumption with spells"],
                "choice_new_slug": "Income or consumption consolidated",
                "replace": True,
                "view_config": {
                    "hideRelativeToggle": False,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                },
            },
        ],
    )

    # Save & upload
    c.save()

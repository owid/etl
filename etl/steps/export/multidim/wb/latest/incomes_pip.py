"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP year
PPP_YEAR = 2021

# Define indicators to use
INDICATORS = ["mean", "median", "avg", "thr", "share"]

# Define dimensions for main views
DIMENSIONS_CONFIG = {
    "decile": "*",
    "period": "*",
    "table": ["Income or consumption consolidated", "Income with spells", "Consumption with spells"],
    "survey_comparability": "*",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("world_bank_pip")
    tb = ds.read("incomes", load_data=False)

    # Remove unwanted dimensions.
    # NOTE: This is a temporary solution until we figure out how to deal with missing dimensions.
    columns_to_keep = []
    for column in tb.drop(columns=["country", "year"]).columns:
        # Keep only indicators for a specific PPP year, and then remove that dimension.
        if ("ppp_version" in tb[column].metadata.dimensions) and tb[column].metadata.dimensions[
            "ppp_version"
        ] == PPP_YEAR:
            columns_to_keep.append(column)
            tb[column].metadata.dimensions.pop("ppp_version")

        # Remove dimensions that are not needed.
        for dimension in ["welfare_type"]:
            if dimension in tb[column].metadata.dimensions:
                tb[column].metadata.dimensions.pop(dimension)
    tb = tb[columns_to_keep]

    # Get all survey_comparability values except "No spells" for spell views
    survey_comp_values = set()
    for col in tb.columns:
        if "survey_comparability" in tb[col].metadata.dimensions:
            survey_comp_values.add(tb[col].metadata.dimensions["survey_comparability"])
    survey_comp_spells = [v for v in survey_comp_values if v != "No spells"]

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="incomes_pip",
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
                "overwrite_dimension_choice": True,
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

    #
    # (optional) Edit views
    #
    for view in c.views:
        # if view.dimension["sex"] == "male":
        #     view.config["title"] = "Something else"
        pass

    #
    # Save garden dataset.
    #
    c.save()

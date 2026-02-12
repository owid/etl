from etl.helpers import PathFinder

paths = PathFinder(__file__)

INDICATORS = ["palma_ratio"]

DIMENSIONS_CONFIG = {
    "table": ["Income or consumption consolidated", "Income with spells", "Consumption with spells"],
    "survey_comparability": "*",
}


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("world_bank_pip")
    tb = ds.read("inequality", load_data=False)

    # Remove unwanted dimensions
    for column in tb.drop(columns=["country", "year"]).columns:
        if tb[column].metadata.dimensions is None:
            continue
        for dimension in ["welfare_type"]:
            if dimension in tb[column].metadata.dimensions:
                tb[column].metadata.dimensions.pop(dimension)

    # Get all survey_comparability values except "No spells" for spell views
    survey_comp_values = set()
    for col in tb.columns:
        if tb[col].metadata.dimensions and "survey_comparability" in tb[col].metadata.dimensions:
            survey_comp_values.add(tb[col].metadata.dimensions["survey_comparability"])
    survey_comp_spells = [v for v in survey_comp_values if v != "No spells"]

    # Create collection
    c = paths.create_collection(
        config=config,
        short_name="palma_ratio_pip",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # Group survey_comparability spells
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

    # Group the table dimension (merge income/consumption spells into consolidated)
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

    c.save()

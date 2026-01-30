"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP year
PPP_YEAR = 2021

# Define indicators to use
INDICATORS = ["mean", "median", "avg", "thr", "share"]

"decile", "period", "table", "survey_comparability"

# Define dimensions
DIMENSIONS_CONFIG = {
    "decile": "*",
    "period": "*",
    "table": ["Income or consumption consolidated"],
    "survey_comparability": ["No spells"],
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

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="pip_incomes",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
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

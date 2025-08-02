from etl.collection import combine_config_dimensions, expand_config

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP year
# NOTE: Update year when prices change
PPP_YEAR = 2021

# NOTE: Update lines when prices change
DIMENSIONS_CONFIG = {
    "poverty_line": ["100", "300", "420", "830", "1000", "2000", "3000", "4000"],
    "table": ["Income or consumption consolidated"],
    # "welfare_type": "*",
    # "decile": "*",
    "survey_comparability": ["No spells"],
}


# etlr multidim
def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("world_bank_pip")
    tb = ds.read("world_bank_pip", load_data=False)

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
        for dimension in ["welfare_type", "decile"]:
            if dimension in tb[column].metadata.dimensions:
                tb[column].metadata.dimensions.pop(dimension)
    tb = tb[columns_to_keep]

    # Bake config automatically from table
    config_new = expand_config(
        tb,  # type: ignore
        indicator_names=[
            "headcount_ratio",
            "headcount",
            # "total_shortfall",
            # "avg_shortfall",
            # "income_gap_ratio",
            # "poverty_gap_index",
        ],
        dimensions=DIMENSIONS_CONFIG,
    )

    # Combine both sources
    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] += config_new["views"]

    # Create mdim
    c = paths.create_collection(
        config=config,
        short_name="poverty",
    )

    # Save & upload
    c.save()

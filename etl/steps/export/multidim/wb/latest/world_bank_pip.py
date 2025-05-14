from etl.collection import combine_config_dimensions, expand_config

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

DIMENSIONS_CONFIG = {
    "poverty_line": ["100", "215", "365", "685", "1000", "2000", "3000", "4000"],
    "table": ["Income or consumption consolidated"],
    # TODO: It seems that the dimension is float, while the original was probably int. Check if this is a bug.
    # "ppp_version": ["2017.0"],
    # "welfare_type": "*",
    # "decile": "*",
    # "survey_comparability": "*",
}


# etlr multidim
def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("world_bank_pip_dimensional")
    tb = ds.read("world_bank_pip_dimensional", load_data=False)

    # Remove unwanted dimensions.
    # NOTE: This is a temporary solution until we figure out how to deal with missing dimensions.
    columns_to_keep = []
    for column in tb.drop(columns=["country", "year"]).columns:
        # Keep only indicators for a specific PPP year, and then remove that dimension.
        if ("ppp_version" in tb[column].metadata.dimensions) and tb[column].metadata.dimensions["ppp_version"] == 2017:
            columns_to_keep.append(column)
            tb[column].metadata.dimensions.pop("ppp_version")
        # Remove dimensions that are not needed.
        for dimension in ["welfare_type", "decile", "survey_comparability"]:
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
    mdim = paths.create_collection(
        config=config,
        short_name="poverty",
    )

    # # Group certain views together: used to create StackedBar charts
    # mdim.group_views(
    #     params=[
    #         {
    #             "dimension": "poverty_line",
    #             "choices": ["100", "215", "365", "685", "1000", "2000", "3000", "4000"],
    #             "choice_new_slug": "multiple_lines_absolute",
    #             "overwrite_dimension_choice": True,
    #         },
    #         {
    #             "dimension": "poverty_line",
    #             "choices": ["40% of the median", "50% of the median", "60% of the median"],
    #             "choice_new_slug": "multiple_lines_relative",
    #             "overwrite_dimension_choice": True,
    #         },
    #     ]
    # )

    # # Edit order of slugs
    # mdim.sort_choices({"poverty_line": lambda x: sorted(x)})

    # Save & upload
    mdim.save()

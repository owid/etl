from etl.collections import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# etlr multidim
def run(dest_dir: str) -> None:
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_coverage")
    tb = ds.read("vaccination_coverage", load_data=True)

    # 2: Bake config automatically from table
    config_coverage = multidim.expand_config(tb, indicator_name="coverage", dimensions=["antigen"])
    config_unvax = multidim.expand_config(tb, indicator_name="unvaccinated_one_year_olds", dimensions=["antigen"])

    # 3: Combine both sources (basically dimensions and views)
    # config["dimensions"] = config_coverage["dimensions"]
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_coverage["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )

    for view in config_coverage["views"]:
        view["dimensions"]["metric"] = "coverage"
    for view in config_unvax["views"]:
        view["dimensions"]["metric"] = "unvaccinated_one_year_olds"

    config["views"] = config_coverage["views"] + config_unvax["views"]

    # 4: Upsert to DB
    multidim.upsert_multidim_data_page(
        mdim_name="mdd-vaccination-who",
        config=config,
        paths=paths,
    )


#    multidim.upsert_multidim_data_page(
#        config=config,
#        paths=paths,
#    )
# config["views"] = multidim.generate_views_for_dimensions(
#        dimensions=config["dimensions"],
#        tables=[tb],
#        dimensions_order_in_slug=("metric", "antigen"),
#        warn_on_missing_combinations=False,
#       additional_config={
#            "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
#            "chartTypes": ["LineChart"],
#            "hasMapTab": True,
#            "tab": "map",
#            "map": {
#                "colorScale": {"baseColorScheme": "YlGbBu"},
#            },
#        },
#    )

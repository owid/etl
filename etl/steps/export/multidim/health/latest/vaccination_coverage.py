from etl.collections import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# etlr multidim
def run(dest_dir: str) -> None:
    # engine = get_engine()
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_coverage")
    tb = ds.read("vaccination_coverage", load_data=False)

    # 2: Bake config automatically from table
    config_new = multidim.expand_config(tb, indicator_names=["coverage"], dimensions=["antigen"])
    # 3: Combine both sources (basically dimensions and views)
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] = config_new["views"]

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

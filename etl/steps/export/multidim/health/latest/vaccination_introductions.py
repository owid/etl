from etl.collections import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "hasMapTab": True,
    "tab": "map",
}


# etlr multidim
def run() -> None:
    # engine = get_engine()
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_introductions")
    tb = ds.read("vaccination_introductions", load_data=True)

    common_view_config = MULTIDIM_CONFIG
    # 2: Bake config automatically from table
    config_new = multidim.expand_config(
        tb,
        indicator_names=["intro"],
        dimensions=["description"],
        indicators_slug="vaccine",
        common_view_config=common_view_config,
    )
    # 3: Combine both sources (basically dimensions and views)
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] = config_new["views"]

    # 4: Upsert to DB
    mdim = paths.create_mdim(
        config=config,
        mdim_name="mdd-vaccination-introductions-who",
    )

    # Edit order of slugs
    mdim.sort_choices({"description": lambda x: sorted(x)})
    mdim.save()

from etl.collections import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# etlr multidim
def run() -> None:
    # engine = get_engine()
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("migration_stock_flows")
    tb = ds.read("migrant_stock_dest_origin")

    # add country names and slugs to the config
    all_countries = tb["country"].unique()
    cty_dict_ls = [{"slug": c.lower(), "name": c} for c in all_countries]
    config["dimensions"][1]["choices"] = cty_dict_ls

    # Define common view configuration
    common_view_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
        "chartTypes": ["LineChart"],
        "hasMapTab": True,
        "tab": "map",
        "map": {
            "colorScale": {
                "binningStrategy": "manual",
                "baseColorScheme": "YlGnBu",
                "customNumericColorsActive": True,
                "customNumericMinValue": 0,
                "customNumericValues": [0, 1000, 10000, 100000, 1000000, 10000000, 0],
                "customNumericColors": ["#AF1629", None, None, None, None, None, None],
                "customNumericLabels": ["Selected Country", None, None, None, None, None, None],
            },
        },
    }

    # 2: Bake config automatically from table
    config_new = multidim.expand_config(
        tb,
        indicator_names=["migrants_all_sexes"],
        dimensions=["metric", "country_select"],
        indicators_slug="migrants",
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
        mdim_name="migration-flows",
    )
    mdim.save()

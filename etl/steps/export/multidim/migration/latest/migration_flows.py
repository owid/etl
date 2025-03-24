from etl.collections import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "chartTypes": ["LineChart"],
    "hasMapTab": True,
    "tab": "map",
    "map": {
        "tooltipUseCustomLabels": True,
        "colorScale": {
            "binningStrategy": "manual",
            "baseColorScheme": "YlGnBu",
            "customNumericMinValue": 0,
            "customNumericValues": [1000, 3000, 10000, 30000, 100000, 300000, 1000000, 0],
            "customCategoryColors": {"Selected country": "#AF1629"},
            "customCategoryLabels": {"Selected country": "Selected country"},
        },
    },
}


# etlr multidim
def run() -> None:
    # engine = get_engine()
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("migration_stock_flows")
    tb = ds.read("migrant_stock_dest_origin", load_data=False)

    # add country names and slugs to the config
    cty_idx = [i for i, d in enumerate(config["dimensions"]) if d["slug"] == "country_select"][0]

    all_countries = [tb[col].dimensions["country_select"] for col in tb.columns if col not in ["year", "country"]]
    all_countries = sorted(list(set(all_countries)))
    cty_dict_ls = [{"slug": c.lower(), "name": c} for c in all_countries]
    config["dimensions"][cty_idx]["choices"] = cty_dict_ls

    # Define common view configuration
    common_view_config = MULTIDIM_CONFIG

    # 2: Bake config automatically from table
    config_new = multidim.expand_config(
        tb,  # type: ignore
        indicator_names=["migrants"],
        dimensions=["country_select", "metric", "gender"],
        common_view_config=common_view_config,
    )

    # 3: Combine both sources
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

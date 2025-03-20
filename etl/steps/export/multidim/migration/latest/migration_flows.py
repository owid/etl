from etl.collections import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Define common view configuration
CHART_CONFIG = {
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


def run() -> None:
    # 1: Load dependencies
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()
    # Load table
    ds = paths.load_dataset("migration_stock_flows")
    tb = ds.read("migration_stock_flows", load_data=False)

    # 2: Bake config automatically from table
    config_new = multidim.expand_config(
        tb,  # type: ignore
        indicator_names=["immigrants", "emigrants"],
        dimensions=["country_origin_or_dest", "gender"],
        common_view_config=CHART_CONFIG,
    )

    # 3: Combine both sources (basically dimensions and views)
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] = config_new["views"]

    # 4: Create MDIM
    mdim = paths.create_mdim(
        config=config,
        mdim_name="migration-flows",
    )

    # 5: Edit order of slugs
    mdim.sort_choices({"country_origin_or_dest": lambda x: sorted(x)})

    # 6: Save
    mdim.save()

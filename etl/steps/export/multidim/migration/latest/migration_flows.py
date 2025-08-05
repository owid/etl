from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "chartTypes": ["LineChart", "DiscreteBar"],
    "hasMapTab": True,
    "tab": "map",
    "map": {
        "tooltipUseCustomLabels": True,
        "colorScale": {
            "binningStrategy": "manual",
            "baseColorScheme": "YlGnBu",
            "customNumericValues": [0, 1000, 3000, 10000, 30000, 100000, 300000, 1000000, 0],
            "customCategoryColors": {"Selected country": "#AF1629"},
            "customCategoryLabels": {"Selected country": "Selected country"},
        },
    },
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("migration_stock_flows")
    tb = ds.read("migrant_stock_dest_origin", load_data=False)

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=tb,
        short_name="migration-flows",
        indicator_names=["migrants"],
        dimensions=["country_select", "metric", "gender"],
        common_view_config=MULTIDIM_CONFIG,
    )
    c.sort_choices({"country_select": lambda x: sorted(x)})

    # Save & upload
    c.save()

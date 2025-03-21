from etl.collections.explorer import combine_config_dimensions, expand_config, hack_metadata_propagation

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

DISPLAY_SETTINGS = {
    "colorScaleNumericMinValue": 0,
    "colorScaleNumericBins": "1000,,;10000,,;100000,,;1000000,,;10000000",
    "colorScaleEqualSizeBins": True,
    "colorScaleScheme": "YlGnBu",
    "colorScaleCategoricalBins": "Selected country,#AF1629,Selected country",
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    ds = paths.load_dataset("migration_stock_flows")
    tb = ds.read("migration_stock_flows", load_data=False)

    # Define common view configuration
    common_view_config = {
        "type": "LineChart",
        "hasMapTab": True,
        "tab": "map",
        "note": 'For most countries, immigrant means "born in another country". Someone who has gained citizenship in the country they live in is still counted as an immigrant if they were born elsewhere. For some countries, place of birth information is not available; in this case citizenship is used to define whether someone counts as an immigrant.',
    }

    # 2: Bake config automatically from table
    config_new = expand_config(
        tb,
        indicator_names=["immigrants", "emigrants"],
        dimensions=["country_origin_or_dest", "gender"],
        common_view_config=common_view_config,
    )

    # 3: Combine both sources (basically dimensions and views)
    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] = config_new["views"]

    # 4: Create explorer
    explorer = paths.create_explorer(
        config=config,
        explorer_name="migration-flows",
    )

    # 5: Edit order of slugs
    explorer.sort_choices({"country_origin_or_dest": lambda x: sorted(x)})

    # 6: Set display settings
    add_display_settings(explorer)

    # 7: HACK
    hack_metadata_propagation(explorer, [tb])

    # 8: Save explorer to DB
    explorer.save()


def add_display_settings(explorer):
    for view in explorer.views:
        # Check that there is only one indicator and is in the y axis.
        assert view.indicators.y is not None, "No y indicator found"
        assert view.num_indicators == 1, "More than one indicator in the view!"

        # Set default display settings
        if view.dimensions["indicator"] == "emigrants":
            view.indicators.y[0].display = DISPLAY_SETTINGS
            # view.config = {}
        elif view.dimensions["indicator"] == "immigrants":
            view.indicators.y[0].display = DISPLAY_SETTINGS

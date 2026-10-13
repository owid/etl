# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

DISPLAY_SETTINGS = {
    "colorScaleNumericMinValue": 0,
    "colorScaleNumericBins": "1000,,;3000,,;10000,,;30000,,;100000,,;300000,,;1000000,,;0",
    "colorScaleScheme": "YlGnBu",
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    # Add views for all dimensions
    ds = paths.load_dataset("migration_stock_flows")
    tb = ds.read("migrant_stock_dest_origin", load_data=False)

    # Define common view configuration
    common_view_config = {
        "type": "LineChart DiscreteBar",
        "hasMapTab": True,
        "tab": "map",
        "note": 'For most countries, immigrant means "born in another country". Someone who has gained citizenship in the country they live in is still counted as an immigrant if they were born elsewhere. For some countries, place of birth information is not available; in this case citizenship is used to define whether someone counts as an immigrant.',
        "map": {
            "colorScale": {
                # "Not applicable" is the bin inapplicableEntityNames (set per view below) renders in.
                "customCategoryColors": {"Not applicable": "#AF1629"},
                "customCategoryLabels": {"Not applicable": "Selected country"},
            },
        },
    }

    c = paths.create_explorer(
        config=config,
        tb=tb,
        indicator_names=["migrants"],
        dimensions=["country_select", "metric", "gender"],
        common_view_config=common_view_config,
        short_name="migration-flows",
    )
    c.sort_choices({"country_select": lambda x: sorted(x)})

    # Set display settings
    add_display_settings(c)

    # Save explorer to DB
    c.save()


def add_display_settings(explorer):
    for view in explorer.views:
        # Check that there is only one indicator and is in the y axis.
        assert view.indicators.y is not None, "No y indicator found"
        assert view.num_indicators == 1, "More than one indicator in the view!"

        # Set default display settings
        view.indicators.y[0].display = DISPLAY_SETTINGS

        # A country has no meaningful "migrants" value for itself (e.g. Afghans who emigrated
        # to Afghanistan), so mark that entity as inapplicable instead of faking a data point.
        # NOTE: don't mutate view.config in place — views share the same common_view_config
        # dict, so an in-place write would leak the last view's value onto every other view.
        view.config = {**(view.config or {}), "inapplicableEntityNames": [view.dimensions["country_select"]]}

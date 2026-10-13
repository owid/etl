from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MULTIDIM_CONFIG = {
    "chartTypes": ["LineChart", "DiscreteBar"],
    "hasMapTab": True,
    "tab": "map",
    "map": {
        "tooltipUseCustomLabels": True,
        "colorScale": {
            "binningStrategy": "manual",
            "baseColorScheme": "YlGnBu",
            "customNumericValues": [0, 1000, 3000, 10000, 30000, 100000, 300000, 1000000, 0],
            # "Not applicable" is the bin inapplicableEntityNames (set per view below) renders in.
            "customCategoryColors": {"Not applicable": "#AF1629"},
            "customCategoryLabels": {"Not applicable": "Selected country"},
        },
    },
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("migration_stock_flows")
    tb = ds.read("migrant_stock_dest_origin", load_data=False)

    # Create chart
    c = paths.create_chart(
        config=config,
        tb=tb,
        short_name="migration-flows",
        indicator_names=["migrants"],
        dimensions=["country_select", "metric", "gender"],
        common_view_config=MULTIDIM_CONFIG,
    )
    c.sort_choices({"country_select": lambda x: sorted(x)})

    # A country has no meaningful "migrants" value for itself (e.g. Afghans who emigrated to
    # Afghanistan), so mark that entity as inapplicable instead of faking a data point for it.
    for view in c.views:
        # NOTE: don't mutate view.config in place — views share the same common_view_config
        # dict, so an in-place write would leak the last view's value onto every other view.
        view.config = {**(view.config or {}), "inapplicableEntityNames": [view.dimensions["country_select"]]}

    # Save & upload
    c.save()

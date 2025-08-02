from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MULTIDIM_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "chartTypes": ["LineChart"],
    "hasMapTab": True,
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("trade_mdim")
    tb = ds.read("trade", load_data=False)

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["value"],
        dimensions=["counterpart_country", "metric"],
        common_view_config=MULTIDIM_CONFIG,
    )
    c.group_views(
        groups=[
            {
                "dimension": "metric",
                "choice_new_slug": "side_by_side",
                "view_config": {
                    "hasMapTab": False,
                    "tab": "chart",
                    "facettingLabelByYVariables": "counterpart_country",
                    "selectedFacetStrategy": "metric",
                },
            },
        ]
    )
    c.sort_choices({"counterpart_country": lambda x: sorted(x)})

    # Save & upload
    c.save()

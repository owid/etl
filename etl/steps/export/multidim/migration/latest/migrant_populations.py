from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Color (region) indicator needed by the Marimekko tab.
REGION_PATH = "regions#owid_region"


def run() -> None:
    c = paths.create_collection(
        config=paths.load_collection_config(),
        short_name="migrant_populations",
    )

    # Add Marimekko as an additional chart type, colored by region.
    for view in c.views:
        view.config = view.config or {}
        view.config["chartTypes"] = ["LineChart", "DiscreteBar", "Marimekko"]
        view.indicators.set_indicator(color=REGION_PATH)
        view.config["matchingEntitiesOnly"] = True
        view.config["showNoDataArea"] = False

    c.save()

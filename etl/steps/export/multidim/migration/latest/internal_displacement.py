from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Set x (population) and color (region) indicators needed by the Marimekko tab.
POPULATION_PATH = "historical#population_historical"
REGION_PATH = "regions#owid_region"


def run() -> None:
    c = paths.create_collection(
        config=paths.load_collection_config(),
        short_name="internal_displacement",
    )

    # Add Marimekko as an additional chart type, sized by population and colored by region.
    for view in c.views:
        view.config = view.config or {}
        view.config["chartTypes"] = ["LineChart", "DiscreteBar", "Marimekko"]
        view.indicators.set_indicator(x=POPULATION_PATH, color=REGION_PATH)
        view.config["matchingEntitiesOnly"] = True

    c.save()

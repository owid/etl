from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Population (x, per-capita views only) and color (region) indicators needed by the Marimekko tab.
POPULATION_PATH = "historical#population_historical"
REGION_PATH = "regions#owid_region"


def run() -> None:
    c = paths.create_collection(
        config=paths.load_collection_config(),
        short_name="internal_displacement",
    )

    # Add Marimekko as an additional chart type, colored by region. Per-capita views are also
    # sized by population, since a rate alone says nothing about the number of people behind it.
    for view in c.views:
        view.config = view.config or {}
        view.config["chartTypes"] = ["LineChart", "DiscreteBar", "Marimekko"]
        if view.matches(unit="per_capita"):
            view.indicators.set_indicator(x=POPULATION_PATH, color=REGION_PATH)
        else:
            view.indicators.set_indicator(color=REGION_PATH)
        view.config["matchingEntitiesOnly"] = True
        view.config["showNoDataArea"] = False

    c.save()

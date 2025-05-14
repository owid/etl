"""This is an example on how you can read another MDIM and create a new one based on it."""

from etl.collection import combine_collections
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    cs = paths.load_collectionset("covid")
    cols = [
        cs.read("covid_cases"),
        cs.read("covid_deaths"),
    ]

    # Combine
    c = combine_collections(
        collections=cols,
        collection_name="test_combined",
        collection_dimension_name="Indicator",
        collection_choices_names=["COVID-19 cases", "COVID-19 deaths"],
        config=paths.load_collection_config(),
        force_collection_dimension=True,
    )

    # Save & upload
    c.save()

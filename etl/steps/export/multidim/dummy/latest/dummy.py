"""This is an example on how you can read another MDIM and create a new one based on it.

TODO: Look in etl.collection.beta for more details.
"""

from etl.collection.beta import combine_mdims
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
    col_name = "test_combined"

    # Combine
    mdim = combine_mdims(
        mdims=cols,
        mdim_name=col_name,
        mdim_dimension_name="Indicator",
        mdim_choices_names=["COVID-19 cases", "COVID-19 deaths"],
        config=paths.load_mdim_config(),
    )

    # Save & upload
    mdim.save()

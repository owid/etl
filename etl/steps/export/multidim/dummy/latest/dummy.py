"""This is an example on how you can read another MDIM and create a new one based on it.

TODO: Look in etl.collections.beta for more details.
"""

from etl.collections.beta import combine_mdims
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    mdims = paths.load_mdims("covid")
    mdims = [
        mdims.read("covid_cases"),
        mdims.read("covid_deaths"),
    ]
    mdim_name = "test_combined"

    # Combine
    mdim = combine_mdims(
        mdims=mdims,
        mdim_name=mdim_name,
        mdim_dimension_name="Indicator",
        mdim_choices_names=["COVID-19 cases", "COVID-19 deaths"],
        config=paths.load_mdim_config(),
    )

    # TODO: Translate MDIM to explorer!

    # Save & upload
    mdim.save()

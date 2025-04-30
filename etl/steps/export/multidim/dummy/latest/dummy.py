"""DEMO for ENG/PE offsite 2025."""

from etl.collections.beta import combine_mdims, mdim_to_explorer
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # 1) Load MDIMs from step.
    mdims = paths.load_mdims("covid")

    # 2) Load MDIMs of interest
    mdims = [
        mdims.read("covid_cases"),
        mdims.read("covid_deaths"),
    ]

    # 3) Combine MDIMs into new one
    mdim = combine_mdims(
        mdims=mdims,
        mdim_name="test_combined",
        mdim_dimension_name="Indicator",
        mdim_choices_names=["COVID-19 cases", "COVID-19 deaths"],
        config=paths.load_mdim_config(),
    )
    # 4) Save MDIM to DB
    mdim.save()

    # 5) Translate MDIM to explorer!
    explorer = mdim_to_explorer(mdim)
    explorer.save()

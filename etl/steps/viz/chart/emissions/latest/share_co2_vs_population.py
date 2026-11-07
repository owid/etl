"""Compare population and territorial emissions shares in their latest complete year."""

from etl.helpers import PathFinder
from etl.steps.viz.chart.emissions.latest.shared import configure_income_comparison

paths = PathFinder(__file__)


def run() -> None:
    # Load the chart and both indicators.
    config = paths.load_config()
    emissions = paths.load_dataset("global_carbon_budget").read("global_carbon_budget")
    population = paths.load_dataset("population").read("population")

    # Require both shares for every selected income group in the same year.
    configure_income_comparison(config, emissions, population, "emissions_total_as_share_of_global")

    # Save the existing chart with the calculated reporting year.
    paths.create_chart(config=config).save()

"""Compare consumption emissions with the World average in a year with country data."""

from etl.helpers import PathFinder
from etl.steps.viz.chart.emissions.latest.shared import configure_average_map, country_names

paths = PathFinder(__file__)


def run() -> None:
    # Load the chart and its emissions data.
    config = paths.load_config()
    tb = paths.load_dataset("global_carbon_budget").read("global_carbon_budget")

    # Country consumption data can end before the World series does.
    configure_average_map(config, tb, "consumption_emissions_per_capita", country_names(paths))

    # Save the existing chart with the calculated settings.
    paths.create_chart(config=config).save()

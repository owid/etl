"""Create the chickens killed for eggs multidimensional chart from its adjacent config file."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    #
    # Save outputs.
    #
    # Create and save the chart.
    chart = paths.create_chart(config=config)
    chart.save()

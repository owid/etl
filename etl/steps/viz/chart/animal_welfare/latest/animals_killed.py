"""Create the multidim of animals killed for food, counted in the country or for the country's consumption."""

from shared import METRICS_ANIMALS_KILLED, create_config

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Short names of the metric choices, so that they fit as toggle buttons.
METRIC_NAMES = {
    "animals_killed": "Production",
    "animals_killed_for_consumption": "Consumption",
}


def run() -> None:
    config = create_config(paths, metrics=METRICS_ANIMALS_KILLED)

    # Show the choice between production and consumption as toggle buttons.
    dimensions = {dimension["slug"]: dimension for dimension in config["dimensions"]}
    metric = dimensions["metric"]
    metric["name"] = "Based on"
    metric["presentation"] = {"type": "toggle-buttons"}
    for choice in metric["choices"]:
        choice["name"] = METRIC_NAMES[choice["slug"]]

    # Show the animal first: a selector only limits the choices of those after it, so every animal stays listed, and
    # the toggle is disabled for animals that have no consumption-based numbers.
    config["dimensions"] = [dimensions["animal"], metric] + [
        dimension for dimension in config["dimensions"] if dimension["slug"] not in ["animal", "metric"]
    ]

    c = paths.create_chart(config=config)
    c.save()

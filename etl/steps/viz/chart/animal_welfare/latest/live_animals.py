"""Create the multidim of live animals, including hens in cages and cage-free systems."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    # Load grapher dataset of animals used for food.
    tb = paths.load_dataset("animals_used_for_food").read("animals_used_for_food", load_data=False)

    #
    # Process data.
    #
    # Create one view for each live animal, in total and per person, with one sub-chart per indicator (the view on hens
    # in cages is defined in the config).
    indicators = ["live_animals", "live_animals_per_capita"]
    c = paths.create_chart(
        config=config,
        tb=[tb] * len(indicators),
        indicator_names=[[indicator] for indicator in indicators],
        dimensions={"metric": "*", "animal": "*", "per_capita": "*", "estimate": "*"},
    )
    # Live animals are not estimated as a range, so the estimate selector is not needed.
    c.dimensions = [dimension for dimension in c.dimensions if dimension.slug != "estimate"]
    for view in c.views:
        del view.dimensions["estimate"]

    #
    # Save outputs.
    #
    c.save()

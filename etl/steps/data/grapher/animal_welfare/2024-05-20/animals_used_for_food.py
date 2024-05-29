"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# List of animals that will be combined into an "other" category.
OTHER_KILLED = [
    # 'chickens',
    # 'ducks',
    # 'pigs',
    # 'geese',
    # 'sheep',
    # 'rabbits',
    # 'turkeys',
    # 'goats',
    # 'cattle',
    "other_rodents",
    "pigeons",
    "buffaloes",
    "horses",
    "camels",
    "donkeys",
    "other_camelids",
    "other_non_mammals",
    "snails",
    "game",
    "mule",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("animals_used_for_food")
    tb = ds_garden["animals_used_for_food"]

    #
    # Process data.
    #
    # For convenience, combine some animals into a single category.
    tb["other_killed"] = tb[[f"{column}_killed" for column in OTHER_KILLED]].sum(axis=1)
    tb["other_killed"].metadata.title = "Number of other animals slaughtered to produce meat"
    tb = tb.drop(columns=[f"{column}_killed" for column in OTHER_KILLED])

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# List of animals most frequently killed (all other will be combined into an "other" category).
MAIN_ANIMALS_KILLED = [
    "chickens",
    "ducks",
    "pigs",
    "geese",
    "sheep",
    "rabbits",
    "turkeys",
    "goats",
    "cattle",
]

# Label for all other animals.
OTHER_ANIMALS_KILLED_LABEL = "other animals"


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("animals_used_for_food")
    tb = ds_garden.read("animals_used_for_food")

    #
    # Process data.
    #
    # Keep only non-per capita rows.
    tb = tb[~tb["per_capita"]].reset_index(drop=True)

    # Group less frequently slaughtered animals into an "other" category.
    tb_other = (
        tb[~tb["animal"].isin(MAIN_ANIMALS_KILLED)]
        .groupby(["country", "year"], as_index=False)
        .agg({"n_animals_killed": "sum", "n_animals_alive": "sum"})
        .assign(**{"animal": OTHER_ANIMALS_KILLED_LABEL})
    )
    tb = pr.concat([tb, tb_other], ignore_index=True)

    # Improve table format.
    tb = tb.format(["country", "year", "animal", "per_capita"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.metadata.title = "Animals used for food (grouped)"
    ds_grapher.save()

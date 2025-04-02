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

# Label for meat total and for mid-point estimate.
# NOTE: These labels should coincide with the ones defined in the garden step of animals_used_for_food (otherwise, an assertion will fail below).
MEAT_TOTAL_LABEL = "all land animals"
ESTIMATE_MIDPOINT_LABEL = "mid-point"

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
    # Sanity checks.
    error = "Label for mid-point estimate may have changed. Update it in the code."
    assert ESTIMATE_MIDPOINT_LABEL in set(tb["estimate"]), error
    error = "Label for all animals (item assigned to total meat) may have changed. Update it in the code."
    assert MEAT_TOTAL_LABEL in set(tb["animal"]), error

    # Keep only non-per capita, mid-point estimate rows.
    tb = tb[(~tb["per_capita"]) & (tb["estimate"] == ESTIMATE_MIDPOINT_LABEL)].reset_index(drop=True)

    # Group less frequently slaughtered animals into an "other" category.
    tb_other = (
        tb[~tb["animal"].isin(MAIN_ANIMALS_KILLED + [MEAT_TOTAL_LABEL])]
        .groupby(["country", "year", "per_capita", "estimate"], as_index=False)
        .agg({"n_animals_killed": "sum", "n_animals_alive": "sum"})
        .assign(**{"animal": OTHER_ANIMALS_KILLED_LABEL})
    )
    tb = pr.concat([tb, tb_other], ignore_index=True)

    # Improve table format.
    tb = tb.format(["country", "year", "animal", "per_capita", "estimate"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.metadata.title = "Animals used for food (grouped)"
    ds_grapher.save()

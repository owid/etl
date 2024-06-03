"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLS_WITH_DATA = [
    "manufactured_cigarettes_millions",
    "manufactured_cigarettes_per_adult_per_day",
    "handrolled_cigarettes_millions",
    "handrolled_cigarettes_per_adult_per_day",
    "total_cigarettes_millions",
    "total_cigarettes_per_adult_per_day",
    "all_tobacco_products_tonnes",
    "all_tobacco_products_grams_per_adult_per_day",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("cigarette_sales")

    # Read table from garden dataset.
    tb = ds_garden["cigarette_sales"].reset_index()

    #
    # Process data.

    # Add East and West Germany together to get values for Germany 1945-1990
    e_ger = tb[tb["country"] == "East Germany"]
    w_ger = tb[tb["country"] == "West Germany"]
    germanies_update = pr.concat([e_ger, w_ger])[COLS_WITH_DATA + ["year"]].groupby("year").sum()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

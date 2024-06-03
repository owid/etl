"""Load a garden dataset and create a grapher dataset."""

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
    ds_garden = paths.garden_dataset

    # Read table from garden dataset.
    tb = ds_garden["cigarette_sales"].reset_index()

    #
    # Process data.

    # include West Germany values for Germany 1945-1990
    tb = tb.replace("West Germany", "Germany")

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    tb = tb.format(["country", "year"])

    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from data, and how to rename them.
COLUMNS = {
    "year": "year",
    "enriched": "number_of_eggs_from_enriched_cages",
    "barn": "number_of_eggs_from_barns",
    "free_range": "number_of_eggs_from_non_organic_free_range_farms",
    "organic": "number_of_eggs_from_organic_free_range_farms",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow: Dataset = paths.load_dependency("uk_egg_statistics")
    tb = ds_meadow["uk_egg_statistics"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Convert million dozens of eggs to eggs.
    for column in tb.drop(columns="year").columns:
        tb[column] *= 12e6

    # Add a country column.
    tb["country"] = "United Kingdom"

    # Add total number of eggs produced.
    # Note: There is no data about organic eggs prior to 2006. Assume they were zero (it probably was close to zero).
    tb["number_of_eggs_all"] = (
        tb["number_of_eggs_from_barns"]
        + tb["number_of_eggs_from_enriched_cages"]
        + tb["number_of_eggs_from_non_organic_free_range_farms"]
        + tb["number_of_eggs_from_organic_free_range_farms"].fillna(0)
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

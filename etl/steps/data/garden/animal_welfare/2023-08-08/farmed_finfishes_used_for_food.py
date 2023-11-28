"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to use and how to rename them.
COLUMNS = {
    "year": "year",
    "n_fish_lower_billions": "n_fish_low",
    "n_fish_midpoint_billions": "n_fish",
    "n_fish_upper_billions": "n_fish_high",
    "production_kilotonnes": "production",
    "production_relative_to_1990_pct": "production_relative_to_1990",
    "n_fish_relative_to_1990_pct": "n_fish_relative_to_1990",
}


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    tb = geo.add_population_to_table(tb, ds_population=ds_population, warn_on_missing_countries=False)
    tb["n_fish_low_per_capita"] = tb["n_fish_low"] / tb["population"]
    tb["n_fish_per_capita"] = tb["n_fish"] / tb["population"]
    tb["n_fish_high_per_capita"] = tb["n_fish_high"] / tb["population"]
    # Drop population column.
    tb = tb.drop(columns=["population"])
    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow: Dataset = paths.load_dependency("farmed_finfishes_used_for_food")
    tb = ds_meadow["farmed_finfishes_used_for_food"].reset_index()

    # Load population dataset.
    ds_population = paths.load_dependency("population")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Adjust units.
    # Kilotonnes to tonnes.
    tb["production"] *= 1e3
    # Number of billions of fishes to number of fishes.
    tb["n_fish_low"] *= 1e9
    tb["n_fish"] *= 1e9
    tb["n_fish_high"] *= 1e9

    # Add a country column.
    tb["country"] = "World"

    # Add per capita number of farmed fish.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset

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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow: Dataset = paths.load_dependency("farmed_finfishes_used_for_food")
    tb = ds_meadow["farmed_finfishes_used_for_food"].reset_index()

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

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("households")

    # Read table from meadow dataset.
    tb = ds_meadow.read("households")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    columns_to_keep = [
        "average_household_size__number_of_members",
        "female_head_of_household__percentage_of_households",
        "one_person",
        "couple_only",
        "couple_with_children",
        "single_parent_with_children",
        "single_mother_with_children",
        "single_father_with_children",
        "extended_family",
        "non_relatives",
        "unknown",
        "nuclear",
        "multi_generation",
        "three_generation",
        "skip_generation",
    ]
    # Replace ".." with NaN
    tb = tb.replace("..", np.nan)

    tb = tb[columns_to_keep + ["country", "year"]]
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

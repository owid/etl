"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Columns to keep (+ their renamings)
COLUMNS_KEEP = {
    "name": "country",
    "year": "year",
    "age_group": "age_group",
    "ctb": "ctb",
    "ctb_rel": "ctb_rel",
}
# Age groups mapping
AGE_GROUPS_MAPPING = {
    1: "0",
    2: "1-14",
    3: "15-39",
    4: "40-59",
    5: "60-79",
    6: "80+",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("le_sex_gap_age_contribution")

    # Read table from meadow dataset.
    tb = ds_meadow["le_sex_gap_age_contribution"].reset_index()

    #
    # Process data.
    #
    # Keep relevant columns
    tb = tb[COLUMNS_KEEP.keys()].rename(columns=COLUMNS_KEEP)

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Age group
    tb["age_group"] = tb["age_group"].map(AGE_GROUPS_MAPPING)

    # Get relative value as a percentage
    tb["ctb_rel"] *= 100

    # Set index
    tb = tb.set_index(["country", "year", "age_group"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

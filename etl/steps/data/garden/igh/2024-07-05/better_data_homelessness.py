"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define scorecard components
SCORECARD_COLUMNS = [
    "methodology_listed",
    "homelessness_enumeration_primary_data_source",
    "enumeration_conducted_within_last_four_years",
    "enumeration_conducted_same_time_of_year",
    "definition_includes_people_without_accommodation",
    "definition_includes_living_in_emergency_accommodation",
    "definition_includes_insecure_or_inadequate_housing",
    "geographic_scope_listed",
    "disaggregation_includes_gender",
    "disaggregation_includes_age",
    "disaggregation_includes_at_least_two",
]

# Define columns to drop
COLUMNS_TO_DROP = [
    "territory_type",
    "population",
    "definition_from_source",
    "notes_on_data",
    "data_source",
    "other_misc_notes",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("better_data_homelessness")

    # Read table from meadow dataset.
    tb = ds_meadow["better_data_homelessness"].reset_index()

    #
    # Process data.
    #
    # For each SCORECARD_COLUMNS, replace "X" with "Yes" and "-" with "No".
    for col in SCORECARD_COLUMNS:
        tb[col] = tb[col].astype("string").str.strip().replace({"X": "Yes", "-": "No"})

    tb = add_igh_framework_category_simplified(tb)

    # Make data_source_type a sentence case.
    tb["data_source_type"] = tb["data_source_type"].str.capitalize()

    # Drop columns in COLUMNS_TO_DROP.
    tb = tb.drop(columns=COLUMNS_TO_DROP)

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
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


def add_igh_framework_category_simplified(tb: Table) -> Table:
    """
    Simplify the igh_framework_category column to these categories
    a) people without accommodation
    b) people living in temporary or crisis accommodation
    c) people living in severely inadequate and insecure accommodation
    d) a+b
    e) a+b+c
    """
    tb["igh_framework_category"] = tb["igh_framework_category"].astype("string")

    tb["igh_framework_category_simplified"] = tb["igh_framework_category"]

    tb.loc[
        tb["igh_framework_category"].str.contains("1"),
        "igh_framework_category_simplified",
    ] = "No accommodation"

    tb.loc[
        tb["igh_framework_category"].str.contains("2"),
        "igh_framework_category_simplified",
    ] = "Temporary and crisis accommodation"

    tb.loc[
        tb["igh_framework_category"].str.contains("3"),
        "igh_framework_category_simplified",
    ] = "Severely inadequate accommodation"

    tb.loc[
        tb["igh_framework_category"].str.contains("1") & tb["igh_framework_category"].str.contains("2"),
        "igh_framework_category_simplified",
    ] = "None or temporary"

    tb.loc[
        tb["igh_framework_category"].str.contains("1") & tb["igh_framework_category"].str.contains("3"),
        "igh_framework_category_simplified",
    ] = "None or inadequate"

    tb.loc[
        tb["igh_framework_category"].str.contains("2") & tb["igh_framework_category"].str.contains("3"),
        "igh_framework_category_simplified",
    ] = "Temporary or inadequate"

    tb.loc[
        tb["igh_framework_category"].str.contains("1")
        & tb["igh_framework_category"].str.contains("2")
        & tb["igh_framework_category"].str.contains("3"),
        "igh_framework_category_simplified",
    ] = "None, temporary or inadequate"

    # Rename category, from "Definition does not align or provide enough detail for IGH Framework classification." to "Not enough information"
    tb.loc[
        tb["igh_framework_category"].str.contains(
            "Definition does not align or provide enough detail for IGH Framework classification."
        ),
        "igh_framework_category_simplified",
    ] = "Not enough information"

    # Drop the original column
    tb = tb.drop(columns=["igh_framework_category"])

    return tb

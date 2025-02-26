"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicator columns and their new names.
INDICATOR_COLUMNS = {
    "Percentage of GDP": "share_of_gdp",
    "Percentage of general government expenditure": "share_of_gov_expenditure",
    "US dollars per person, PPP converted": "usd_per_person_ppp",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("social_expenditure")

    # Read table from meadow dataset.
    tb = ds_meadow.read("social_expenditure")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Pivot table to have indicator as columns.
    tb = tb.pivot(
        index=["country", "year", "expenditure_source", "spending_type", "programme_type_category", "programme_type"],
        columns="indicator",
        values="value",
        join_column_levels_with="_",
    )

    # Rename columns
    tb = tb.rename(columns=INDICATOR_COLUMNS)

    tb = tb.format(
        ["country", "year", "expenditure_source", "spending_type", "programme_type_category", "programme_type"]
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicator columns and their new names
INDICATOR_COLUMNS = {
    "rev": "revenue",
    "exp": "expenditure",
    "ie": "interest_expense",
    "prim_exp": "primary_expenditure",
    "pb": "primary_balance",
    "debt": "gross_debt",
    "rltir": "real_long_term_interest_rate",
    "rgc": "real_growth_rate",
    "gg_budg": "gg_budg",
    "gg_debt": "gg_debt",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("public_finances_modern_history")

    # Read table from meadow dataset.
    tb = ds_meadow["public_finances_modern_history"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Drop ifscode and isocode columns.
    tb = tb.drop(columns=["ifscode", "isocode"], errors="raise")

    # Rename columns
    tb = tb.rename(columns=INDICATOR_COLUMNS, errors="raise")

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

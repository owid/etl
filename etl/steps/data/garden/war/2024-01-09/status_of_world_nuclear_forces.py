"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to read from the data, and how to rename them.
COLUMNS = {
    "country": "country",
    "year": "year",
    "deployed_strategic": "deployed_strategic",
    "deployed_nonstrategic": "deployed_nonstrategic",
    "reserve_nondeployed": "reserve_nondeployed",
    "military_stockpile__a": "stockpile",
    "total_inv": "inventory",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("status_of_world_nuclear_forces")
    tb = ds_meadow["status_of_world_nuclear_forces"].reset_index()

    #
    # Process data.
    #
    # Rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Remove annotations and thousand separators from data values, and set an appropriate type.
    for column in tb.drop(columns=["country", "year"]).columns:
        tb[column] = tb[column].str.replace(r"\D", "", regex=True).astype(float)

    # Looking at the original dashboard, it seems that missing values are shown as zeros.
    # https://public.tableau.com/app/profile/kate.kohn/viz/EstimatedGlobalNuclearWarheadInventories2021/Dashboard1
    tb = tb.fillna(0)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Sanity check.
    error = "Column 'stockpile' should be the sum of deployed and reserve nuclear weapons."
    assert (
        tb["stockpile"] == tb[["deployed_strategic", "deployed_nonstrategic", "reserve_nondeployed"]].sum(axis=1)
    ).any(), error

    # Add column for retired nuclear weapons, which is the total inventory minus the stockpile.
    tb["retired"] = tb["inventory"] - tb["stockpile"]

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

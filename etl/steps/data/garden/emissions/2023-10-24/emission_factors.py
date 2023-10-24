"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Rows to select from the "description" column.
SELECTED_DESCRIPTION = "CO2 Emission Factor for Stationary Combustion (kg/TJ on a net calorific basis)"

# Unit conversion factors.
KILOGRAMS_PER_TERAJOULE_TO_KILOGRAMS_PER_MEGAWATT_HOUR = 3600 / 1e6

# Columns to select and how to rename them.
COLUMNS = {
    "gas": "gas",
    "fuel_2006": "fuel",
    "unit": "unit",
    "value": "emission_factor",
    "source_of_data": "source_of_data",
    "description": "description",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("emission_factors")
    tb = ds_meadow["emission_factors"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Select rows.
    tb = tb[tb["description"] == SELECTED_DESCRIPTION].reset_index(drop=True)

    # Sanity checks.
    error = "Unexpected sources, units, or gas in selected rows."
    assert (
        tb["source_of_data"].str.startswith("2006 IPCC Guidelines for National Greenhouse Gas Inventories").all()
    ), error
    assert tb["gas"].str.startswith("CARBON DIOXIDE").all(), error
    assert (tb["unit"] == "kg/TJ").all(), error

    # Converted units from kilograms per terajoule to kilograms per MWh (kg/MWh).
    tb["emission_factor"] = (
        tb["emission_factor"].astype(str).astype(float) * KILOGRAMS_PER_TERAJOULE_TO_KILOGRAMS_PER_MEGAWATT_HOUR
    )

    # Drop unnecessary columns.
    tb = tb.drop(columns=["source_of_data", "description", "unit", "gas"], errors="raise")

    # Drop repeated rows.
    tb = tb.drop_duplicates().reset_index(drop=True)

    # Clean spurious symbols in the fuel names.
    tb["fuel"] = tb["fuel"].str.replace("\n", " ").str.replace("&", "and").str.strip()

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["fuel"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

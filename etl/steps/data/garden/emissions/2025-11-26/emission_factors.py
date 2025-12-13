"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Rows to select from the "description" column.
SELECTED_DESCRIPTION = "CO2 Emission Factor for Stationary Combustion (kg/TJ on a net calorific basis)"

# Unit conversion factors.
KILOGRAMS_PER_TERAJOULE_TO_KILOGRAMS_PER_MEGAWATT_HOUR = 3600 / 1e6

# Columns to select and how to rename them.
COLUMNS = {
    "gas": "gas",
    "fuel_2006": "source",
    "unit": "unit",
    "value": "emission_factor",
    "source_of_data": "source_of_data",
    "description": "description",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset of energy emission factors and read its main table.
    ds_energy = paths.load_dataset("emission_factors")
    tb_energy = ds_energy.read("emission_factors")

    # Load meadow dataset of electricity emission factors and read its main table.
    ds_electricity = paths.load_dataset("electricity_emission_factors")
    tb_electricity = ds_electricity.read("electricity_emission_factors")

    #
    # Process data.
    #
    # Select and rename columns.
    tb_energy = tb_energy[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Select rows.
    tb_energy = tb_energy[tb_energy["description"] == SELECTED_DESCRIPTION].reset_index(drop=True)

    # Sanity checks.
    error = "Unexpected sources, units, or gas in selected rows."
    assert (
        tb_energy["source_of_data"].str.startswith("2006 IPCC Guidelines for National Greenhouse Gas Inventories").all()
    ), error
    assert tb_energy["gas"].str.startswith("CARBON DIOXIDE").all(), error
    assert (tb_energy["unit"] == "kg/TJ").all(), error

    # Converted units from kilograms per terajoule to kilograms per MWh (kg/MWh).
    tb_energy["emission_factor"] = (
        tb_energy["emission_factor"].astype(str).astype(float) * KILOGRAMS_PER_TERAJOULE_TO_KILOGRAMS_PER_MEGAWATT_HOUR
    )

    # Drop unnecessary columns.
    tb_energy = tb_energy.drop(columns=["source_of_data", "description", "unit", "gas"], errors="raise")

    # Drop repeated rows.
    tb_energy = tb_energy.drop_duplicates().reset_index(drop=True)

    # Clean spurious symbols in the fuel names.
    tb_energy["source"] = tb_energy["source"].str.replace("\n", " ").str.replace("&", "and").str.strip()

    # Improve table formats.
    tb_energy = tb_energy.format(keys=["source"], short_name="energy_emission_factors")
    tb_electricity = tb_electricity.format(keys=["source"], short_name="electricity_emission_factors")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_energy, tb_electricity], default_metadata=ds_energy.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

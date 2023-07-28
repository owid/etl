import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)

# Conversion factor from million tonnes of oil equivalent to terawatt-hours.
MTOE_TO_TWH = 11.63


def combine_tables(tb_fuel_input: Table, tb_supply: Table, tb_efficiency: Table) -> Table:
    """Combine tables (each one originally coming from a different sheet of the BEIS data file) and prepare output table
    with metadata.

    Parameters
    ----------
    tb_fuel_input : Table
        Data extracted from the "Fuel input" sheet.
    tb_supply : Table
        Data extracted from the "Supply, availability & consump" sheet.
    tb_efficiency : Table
        Data (on implied efficiency) extracted from the "Generated and supplied" sheet.

    Returns
    -------
    tb_combined : Table
        Combined and processed table with metadata and a verified index.

    """
    tb_fuel_input = tb_fuel_input.copy()
    tb_supply = tb_supply.copy()
    tb_efficiency = tb_efficiency.copy()

    # Remove rows with duplicated year.
    tb_fuel_input = tb_fuel_input.drop_duplicates(subset="year", keep="last").reset_index(drop=True)
    tb_supply = tb_supply.drop_duplicates(subset="year", keep="last").reset_index(drop=True)
    tb_efficiency = tb_efficiency.drop_duplicates(subset="year", keep="last").reset_index(drop=True)

    # Convert units of fuel input data.
    for column in tb_fuel_input.set_index("year").columns:
        tb_fuel_input[column] *= MTOE_TO_TWH

    # Combine dataframes.
    tb_combined = pr.merge(tb_fuel_input, tb_supply, how="outer", on="year", short_name=paths.short_name)
    tb_combined = pr.merge(tb_combined, tb_efficiency, how="outer", on="year")

    # Add a country column (even if there is only one country).
    tb_combined["country"] = "United Kingdom"

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return tb_combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load meadow dataset and read its tables.
    ds_meadow: Dataset = paths.load_dependency("uk_historical_electricity")
    tb_fuel_input = ds_meadow["fuel_input"].reset_index()
    tb_supply = ds_meadow["supply"].reset_index()
    tb_efficiency = ds_meadow["efficiency"].reset_index()

    #
    # Process data.
    #
    # Clean and combine tables.
    tb = combine_tables(tb_fuel_input=tb_fuel_input, tb_supply=tb_supply, tb_efficiency=tb_efficiency)

    #
    # Save outputs.
    #
    # Create new dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()

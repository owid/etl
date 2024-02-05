"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to read from the data, and how to rename them.
COLUMNS_CO2 = {
    "year": "year",
    "antarctic_ice_cores": "co2_concentration",
}
COLUMNS_CH4 = {
    "year": "year",
    "epica_dome_c__antarctica": "ch4_concentration",
}
COLUMNS_N2O = {
    "year": "year",
    "epica_dome_c__antarctica": "n2o_concentration",
}


def approximate_data_for_each_year(tb: Table, column: str) -> Table:
    tb = tb.copy()

    # Round each year to its closer integer.
    tb["year"] = tb["year"].round(0).astype(int)

    # If there are multiple rows for a given year, take the average value.
    tb = tb.groupby("year", as_index=False).agg({column: "mean"})

    # Remove empty rows.
    tb = tb.dropna(subset=[column]).reset_index(drop=True)

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("ghg_concentration")
    tb_co2 = ds_meadow["co2_concentration"].reset_index()
    tb_ch4 = ds_meadow["ch4_concentration"].reset_index()
    tb_n2o = ds_meadow["n2o_concentration"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb_co2 = tb_co2[list(COLUMNS_CO2)].rename(columns=COLUMNS_CO2, errors="raise")
    tb_ch4 = tb_ch4[list(COLUMNS_CH4)].rename(columns=COLUMNS_CH4, errors="raise")
    tb_n2o = tb_n2o[list(COLUMNS_N2O)].rename(columns=COLUMNS_N2O, errors="raise")

    # Since pandas datetime cannot handle such long past dates, for simplicity, round up years, and take average
    # concentration of year for which there are multiple rows.
    tb_co2 = approximate_data_for_each_year(tb_co2, "co2_concentration")
    tb_ch4 = approximate_data_for_each_year(tb_ch4, "ch4_concentration")
    tb_n2o = approximate_data_for_each_year(tb_n2o, "n2o_concentration")

    # Combine data for all gases.
    tb = tb_co2.merge(tb_ch4, on="year", how="outer").merge(tb_n2o, on="year", how="outer", short_name=paths.short_name)

    # Set an appropriate index to each table and sort conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

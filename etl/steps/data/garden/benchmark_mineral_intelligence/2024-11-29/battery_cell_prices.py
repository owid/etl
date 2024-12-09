"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Select and rename columns.
COLUMNS_ANNUAL = {
    "year": "year",
    "global_avg__cell_price__dollar_kwh": "battery_cell_price",
}
COLUMNS_QUARTERLY = {
    "date": "date",
    "ncm_weighted_average_cell_price": "ncm_battery_cell_price",
    "lfp_weighted_average_cell_price": "lfp_battery_cell_price",
}
# Mapping of battery chemistries.
CHEMISTRY_MAPPING = {
    "battery_cell_price": "Average",
    "ncm_battery_cell_price": "NCM",
    "lfp_battery_cell_price": "LFP",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("battery_cell_prices")

    # Read table on annual data of historical battery prices (since 2014).
    tb_annual = ds_meadow.read("battery_cell_prices")

    # Read table on quarterly data of battery prices by chemistry.
    tb_quarterly = ds_meadow.read("battery_cell_prices_by_chemistry")

    #
    # Process data.
    #
    # Process annual data on historical battery prices.

    # Select and rename columns.
    tb_annual = tb_annual[COLUMNS_ANNUAL.keys()].rename(columns=COLUMNS_ANNUAL, errors="raise")

    # Clean year column.
    tb_annual["year"] = tb_annual["year"].str.strip().str[0:4].astype("Int64")

    # Add country column.
    tb_annual["country"] = "World"

    # Process quarterly data on battery prices by chemistry.

    # Select and rename columns.
    tb_quarterly = tb_quarterly[COLUMNS_QUARTERLY.keys()].rename(columns=COLUMNS_QUARTERLY, errors="raise")

    # Clean date column.
    quarter_to_date = {"Q1": "-02-15", "Q2": "-05-15", "Q3": "-08-15", "Q4": "-11-15"}
    tb_quarterly["date"] = [date[-4:] + quarter_to_date[date[:2]] for date in tb_quarterly["date"]]

    # Add country column.
    tb_quarterly["country"] = "World"

    # Create a combined table.

    # For annual data, assume the date is July 1st of each year.
    _tb_annual = tb_annual.copy()
    _tb_annual["date"] = _tb_annual["year"].astype(str) + "-07-01"

    # Combine tables.
    tb_combined = pr.concat([_tb_annual.drop(columns=["year"]), tb_quarterly])

    # Remove country column, and use the battery chemistry as "country" instead.
    tb_combined = tb_combined.drop(columns=["country"]).melt(id_vars=["date"], var_name="chemistry", value_name="price")

    # Rename battery chemistries.
    tb_combined["chemistry"] = map_series(
        tb_combined["chemistry"], CHEMISTRY_MAPPING, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )

    # Remove empty rows.
    tb_combined = tb_combined.dropna().reset_index(drop=True)

    # Improve table formats.
    tb_annual = tb_annual.format(["country", "year"])
    tb_quarterly = tb_quarterly.format(["country", "date"], short_name="battery_cell_prices_by_chemistry")
    tb_combined = tb_combined.format(["chemistry", "date"], short_name="battery_cell_prices_combined")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_annual, tb_quarterly, tb_combined],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()

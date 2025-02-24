"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Select and rename columns.
COLUMNS = {
    "country": "country",
    "date": "date",
    "price__eur_mwhe": "price",
}

# Minimum number of countries that must be informed per month (applied to PPI data).
# If the number is smaller than this, the month is removed from the data.
# We do this to avoid having very sparse data (especially on the latest informed month).
MIN_NUM_COUNTRIES_INFORMED_PER_MONTH = 10

# Allow PPI data to lag a certain number of months behind, and warn if that lag is larger than expected.
# NOTE: We will also assert that the minimum date in Ember is fully covered by PPI data.
PPI_ALLOWED_MONTHS_OF_LAG = 2

# Base year for the PPI data.
PPI_EUROS_YEAR = 2021


def adjust_prices_for_inflation(tb_monthly: Table, tb_ppi: Table) -> Table:
    # Select Producer Prices Index for classification "[MIG_NRG] MIG - energy".
    tb_ppi = tb_ppi[tb_ppi["classification"] == "MIG_NRG"].drop(columns=["classification"]).reset_index(drop=True)

    # Adapt dates in PPI dataset to match the monthly electricity prices.
    tb_ppi["date"] = tb_ppi["date"].str.strip() + "-01"
    assert tb_ppi["date"].str.len().eq(10).all(), "Unexpected date format in PPI dataset."

    # Remove months for which we don't have enough countries.
    # This happens at least to the most recently informed month, where only a few countries are displayed.
    tb_ppi = tb_ppi[
        tb_ppi.groupby(["date"])["country"].transform("count") > MIN_NUM_COUNTRIES_INFORMED_PER_MONTH
    ].reset_index(drop=True)

    # Combine energy prices table with PPI table.
    tb_monthly = tb_monthly.merge(tb_ppi, on=["country", "date"], how="left")

    # Sanity checks.
    # Check that the maximum date of PPI is only a certain number of months behind Ember data.
    ember_first_month = tb_monthly[tb_monthly["price"].notnull()]["date"].min()
    ember_latest_month = tb_monthly[tb_monthly["price"].notnull()]["date"].max()
    ppi_first_month = tb_monthly[tb_monthly["ppi"].notnull()]["date"].min()
    ppi_latest_month = tb_monthly[tb_monthly["ppi"].notnull()]["date"].max()
    if pd.to_datetime(ppi_latest_month) < pd.to_datetime(ember_latest_month) - pd.DateOffset(
        months=PPI_ALLOWED_MONTHS_OF_LAG
    ):
        log.warning(f"PPI data is lagging behind more than {PPI_ALLOWED_MONTHS_OF_LAG} months behind Ember's data.")
    # Check that the minimum date of PPI fully covers the data in the energy prices table.
    error = "PPI data does not cover the minimum date of energy prices."
    assert ember_first_month >= ppi_first_month, error
    error = "Base year is not as expected"
    import re

    base_year = re.search(r"\b(20\d{2}|19\d{2})\b", tb_ppi["ppi"].metadata.description_short).group(0)
    assert base_year == str(PPI_EUROS_YEAR), error

    # Adjust monthly prices for inflation.
    # NOTE: When doing this, many prices will be lost (e.g. UK data).
    tb_monthly["price"] = tb_monthly["price"] * 100 / tb_monthly["ppi"]

    return tb_monthly


def prepare_annual_data(tb_monthly: Table) -> Table:
    # Ember provides monthly data, so we can create a monthly table of wholesale electricity prices.
    # But we also need to create an annual table of average wholesale electricity prices.
    tb_annual = tb_monthly.copy()
    tb_annual["year"] = tb_annual["date"].str[:4].astype("Int64")
    # NOTE: We will include only complete years. This means that the latest year will not be included. But also, we will disregard country-years like Ireland 2022, which only has data for a few months, for some reason.
    n_months = tb_annual.groupby(["country", "year"], observed=True, as_index=False)["date"].transform("count")
    tb_annual = (
        tb_annual[n_months == 12].groupby(["country", "year"], observed=True, as_index=False).agg({"price": "mean"})
    )

    return tb_annual


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset, and read its main table.
    ds_meadow = paths.load_dataset("european_wholesale_electricity_prices")
    tb_monthly = ds_meadow.read("european_wholesale_electricity_prices")

    # Load Eurostat Producer Prices in Industry dataset, and read its main table.
    ds_ppi = paths.load_dataset("producer_prices_in_industry")
    tb_ppi = ds_ppi.read("producer_prices_in_industry")

    #
    # Process data.
    #
    # Select and rename columns.
    tb_monthly = tb_monthly[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb_monthly = geo.harmonize_countries(df=tb_monthly, countries_file=paths.country_mapping_path)

    # Adjust prices for inflation.
    tb_monthly = adjust_prices_for_inflation(tb_monthly=tb_monthly, tb_ppi=tb_ppi)

    # Prepare annual data.
    tb_annual = prepare_annual_data(tb_monthly=tb_monthly)

    # Improve table formats.
    tb_monthly = tb_monthly.format(["country", "date"], short_name="european_wholesale_electricity_prices_monthly")
    tb_annual = tb_annual.format(short_name="european_wholesale_electricity_prices_annual")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_monthly, tb_annual],
        check_variables_metadata=True,
        yaml_params={"EUROS_YEAR": PPI_EUROS_YEAR},
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

"""This step takes the Global Carbon Budget and GDP data from World Bank's World Development Indicators, and creates a
dataset with the changes in emissions and GDP over time.

"""
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from WDI, and how to rename them.
COLUMNS_WDI = {
    "country": "country",
    "year": "year",
    # GDP, PPP (constant 2017 international $)
    "ny_gdp_mktp_pp_kd": "gdp",
    # GDP per capita, PPP (constant 2017 international $)
    "ny_gdp_pcap_pp_kd": "gdp_per_capita",
}

# Columns to select from GCB, and how to rename them.
COLUMNS_GCB = {
    "country": "country",
    "year": "year",
    "emissions_total": "production_emissions",
    "emissions_total_per_capita": "production_emissions_per_capita",
    "consumption_emissions": "consumption_emissions",
    "consumption_emissions_per_capita": "consumption_emissions_per_capita",
    # 'emissions_total_including_land_use_change': "",
    # 'emissions_total_including_land_use_change_per_capita': "",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load Global Carbon Budget dataset and read its main table.
    ds_gcb = paths.load_dataset("global_carbon_budget")
    tb_gcb = ds_gcb["global_carbon_budget"].reset_index()

    # Load WDI dataset, read its main table.
    ds_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_wdi["wdi"].reset_index()

    #
    # Process data.
    #
    # Select and rename the required variables from GCB.
    tb_gcb = tb_gcb[list(COLUMNS_GCB)].rename(columns=COLUMNS_GCB, errors="raise")

    # Select and rename the required variables from WDI.
    tb_wdi = tb_wdi[list(COLUMNS_WDI)].rename(columns=COLUMNS_WDI, errors="raise")

    # Combine both tables.
    tb = tb_gcb.merge(tb_wdi, on=["country", "year"], how="outer", short_name=paths.short_name)

    # Remove empty rows.
    tb = tb.dropna(
        subset=[column for column in tb.columns if column not in ["country", "year"]], how="all"
    ).reset_index(drop=True)

    # TODO: Decide a start year, and add percent changes (at least in consumption-based emissions and per capita gdp).

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

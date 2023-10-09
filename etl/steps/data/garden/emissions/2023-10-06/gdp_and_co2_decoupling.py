"""This step takes the Global Carbon Budget and GDP data from World Bank's World Development Indicators, and creates a
dataset with the changes in emissions and GDP over time.

"""
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# First and final years to consider.
# Percentage changes will start from START_YEAR, START_YEAR + 1, ..., END_Year - 1, and end in END_YEAR.
START_YEAR = 1990
END_YEAR = 2020

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

    ####################################################################################################################
    # TODO: Remote this temporary solution once WDI has origins.
    from etl.data_helpers.misc import add_origins_to_wdi

    tb_wdi = add_origins_to_wdi(tb_wdi=tb_wdi)
    ####################################################################################################################

    # Combine both tables.
    tb = tb_gcb.merge(tb_wdi, on=["country", "year"], how="outer", short_name=paths.short_name)

    # Define list of non-index columns.
    data_columns = [column for column in tb.columns if column not in ["country", "year"]]

    # Remove empty rows.
    tb = tb.dropna(subset=data_columns, how="all").reset_index(drop=True)

    # Select years between START_YEAR and END_YEAR.
    tb = tb[(tb["year"] >= START_YEAR) & (tb["year"] <= END_YEAR)].reset_index(drop=True)

    # Select data for all countries at the final year.
    tb_final = tb[tb["year"] == END_YEAR].reset_index(drop=True)

    # Add columns for data on the final year to the main table.
    tb = tb.merge(tb_final, on="country", how="left", suffixes=("", "_final_year"))

    # Add percent changes.
    for column in data_columns:
        tb[f"{column}_change"] = (tb[f"{column}_final_year"] - tb[column]) / tb[column] * 100

    # Remove unnecessary columns.
    tb = tb.drop(columns=[f"{column}_final_year" for column in data_columns])

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

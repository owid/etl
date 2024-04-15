"""This step takes the Global Carbon Budget and GDP data from World Bank's World Development Indicators, and creates a
dataset with the changes in emissions and GDP over time.

We already have an interactive chart showing similar data,
for per capita GDP and per capita, consumption-based CO2 emissions:
https://ourworldindata.org/grapher/co2-emissions-and-gdp

The data in the current step is not used by any grapher step, but will be used by the following static chart:
TODO: Include link to the updated static chart once it is created.

"""

from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# First and final years whose (per capita) GDP and emissions will be compared.
START_YEAR = 2006
END_YEAR = 2021

# Columns to select from WDI, and how to rename them.
COLUMNS_WDI = {
    "country": "country",
    "year": "year",
    # GDP, PPP (constant 2017 international $)
    # "ny_gdp_mktp_pp_kd": "gdp",
    # GDP per capita, PPP (constant 2017 international $)
    "ny_gdp_pcap_pp_kd": "gdp_per_capita",
}

# Columns to select from GCB, and how to rename them.
COLUMNS_GCB = {
    "country": "country",
    "year": "year",
    # "emissions_total": "production_emissions",
    # "emissions_total_per_capita": "production_emissions_per_capita",
    # "consumption_emissions": "consumption_emissions",
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
    tb_start = tb[(tb["year"] == START_YEAR)].reset_index(drop=True)

    # Select data for all countries at the final year.
    tb_end = tb[tb["year"] == END_YEAR].reset_index(drop=True)

    # Add columns for data on the final year to the main table.
    tb = tb_start.merge(tb_end, on="country", how="left", suffixes=("_start_year", "_final_year"))

    # Add percent changes.
    for column in data_columns:
        tb[f"{column}_change"] = (
            (tb[f"{column}_final_year"] - tb[f"{column}_start_year"]) / tb[f"{column}_start_year"] * 100
        )

    # Remove unnecessary columns.
    tb = tb.drop(columns=[column for column in tb.columns if "year" in column])

    # Drop rows that miss any of the main columns.
    tb = tb.dropna(how="any").reset_index(drop=True)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True, formats=["csv"])
    ds_garden.save()


# To quickly inspect the decoupling of GDP per capita vs consumption-based emissions per capita, use this function.
# def plot_decoupling(tb, countries=None):
#     import plotly.express as px
#     import owid.catalog.processing as pr
#     from tqdm.auto import tqdm

#     column = "gdp_per_capita_change"
#     emissions_column = "consumption_emissions_per_capita_change"
#     _tb = tb.reset_index().astype({"country": str})[["country", column, emissions_column]]
#     _tb["year"] = START_YEAR
#     if countries is None:
#         countries = sorted(set(_tb["country"]))
#     for country in tqdm(countries):
#         tb_old = _tb[_tb["country"] == country].reset_index(drop=True)
#         if (tb_old[emissions_column].isna().all()) or (tb_old[column].isna().all()):
#             continue
#         title = tb_old[column].metadata.title or column
#         tb_new = tb_old.copy()
#         tb_new["year"] = END_YEAR
#         tb_old[column] = 0
#         tb_old[emissions_column] = 0
#         tb_plot = pr.concat([tb_old, tb_new], ignore_index=True)
#         tb_plot = tb_plot.melt(id_vars=["country", "year"], var_name="Indicator")
#         plot = px.line(tb_plot, x="year", y="value", color="Indicator", title=f"{country} - {title}")
#         plot.show()

# List of countries currently considered for the static chart:
# countries = ["Ireland", "Finland", "Sweden", "Denmark", "Netherlands", "Estonia", "United States", "Canada", "Germany",
# "Belgium", "New Zealand", "Israel", "Japan", "Singapore", "Dominican Republic", "Hungary", "Australia", "Zimbabwe",
# "Ukraine", "Bulgaria", "Switzerland", "Hong Kong", "Slovakia", "Romania", "Czechia", "Nicaragua", "Nigeria",
# "Azerbaijan", "Slovenia", "Croatia"]
# Check that the chosen countries still fulfil the expected conditions.
# print("Countries in the list where GDP has increased less than 5% or emissions have decreased less than 5%:")
# for c in countries:
#     if not tb.loc[c]["consumption_emissions_per_capita_change"] < -5:
#         print("emissions", c, tb.loc[c]["consumption_emissions_per_capita_change"])
#     if not tb.loc[c]["gdp_per_capita_change"] > 5:
#         print("gdp", c, tb.loc[c]["gdp_per_capita_change"])

# If not, print other countries that do fulfil the conditions and are not in the chart.
# other_countries = sorted(set(tb.index) - set(countries))
# for c in other_countries:
#     if (tb.loc[c]["consumption_emissions_per_capita_change"] < -5) and (tb.loc[c]["gdp_per_capita_change"] > 5):
#         print(c, f' -> GDP: {tb.loc[c]["gdp_per_capita_change"]: .1f}%, Emissions: {tb.loc[c]["consumption_emissions_per_capita_change"]:.1f}%')

# plot_decoupling(tb, countries=countries)

"""Extract global (as well as at the country level for some countries) weighted-average levelized cost of electricity
(LCOE) for all energy sources from IRENA's Renewable Power Generation Costs 2022 dataset.

Extract solar photovoltaic module prices too.

NOTE: The original data is poorly formatted. Each energy source is given as a separate sheet, with a different
structure. So it's likely that, on the next update, this script will not work.

"""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_solar_pv_module_prices(data: pr.ExcelFile) -> Table:
    """Prepare yearly data on average solar photovoltaic module prices.

    Monthly data will be averaged, and only complete years (with 12 informed months) will be considered.

    Parameters
    ----------
    data : pr.ExcelFile
        Raw data.

    Returns
    -------
    pv_prices : Table
        PV prices.

    """
    # Photovoltaic technologies to choose for average monthly prices.
    pv_technologies = ["Thin film a-Si/u-Si or Global Index (from Q4 2013)"]

    # Load upper table in sheet from Figure 3.2, which is:
    # Average monthly solar PV module prices by technology and manufacturing country sold in Europe, 2010 to 2021.
    pv_prices = data.parse(sheet_name="Fig 3.2", skiprows=7)
    pv_prices = pv_prices.drop(
        columns=[column for column in pv_prices.columns if "Unnamed" in str(column)], errors="raise"
    )

    # Rename table.
    pv_prices.metadata.short_name = "solar_photovoltaic_module_prices"

    # Transpose table so that each row corresponds to a month.
    pv_prices = pv_prices.rename(columns={"Technology": "technology"}, errors="raise").melt(
        id_vars="technology", var_name="month", value_name="cost"
    )

    # Select PV technologies.
    pv_prices = pv_prices[pv_prices["technology"].isin(pv_technologies)].reset_index(drop=True)

    # Get year from dates.
    pv_prices["year"] = pd.to_datetime(pv_prices["month"], format="%b %y").dt.year

    # For each year get the average cost over all months.
    pv_prices = (
        pv_prices.groupby(["technology", "year"])
        .agg({"cost": "mean", "year": "count"})
        .rename(columns={"year": "n_months"})
        .reset_index()
    )

    # Remove unnecessary column and add column for region.
    pv_prices = pv_prices.drop(columns="technology", errors="raise").assign(**{"country": "World"})

    # Sanity check.
    error = "Incomplete years (with less than 12 months of data) were expected to be either the first or the last."
    assert pv_prices[pv_prices["n_months"] != 12].index.isin([0, len(pv_prices) - 1]).all(), error

    # Ignore years for which we don't have 12 months.
    pv_prices = pv_prices[pv_prices["n_months"] == 12].drop(columns=["n_months"], errors="raise").reset_index(drop=True)

    # Set an appropriate index and sort conveniently.
    pv_prices = pv_prices.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return pv_prices


def extract_global_cost_for_all_sources_from_excel_file(data: pr.ExcelFile) -> Table:
    """Extract global weighted-average LCOE of all energy sources from the excel file.

    Each energy source is given in a separate sheet, in a different way, to each needs a different treatment.

    Parameters
    ----------
    data : pr.ExcelFile
        Raw data.

    Returns
    -------
    tb : Table
        LCOE for different energy sources.
    """
    # Extract weighted average LCOE for different sources (each one requires a slightly different processing):

    # Solar photovoltaic.
    solar_pv = (
        data.parse("Fig 3.1", skiprows=22)
        .dropna(how="all", axis=1)
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")  # type: ignore
    )
    solar_pv = solar_pv[solar_pv["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
    solar_pv["technology"] = "Solar photovoltaic"

    # Onshore wind.
    onshore_wind = (
        data.parse("Fig 2.11", skiprows=3)
        .drop(columns="Unnamed: 0", errors="raise")
        .rename(  # type: ignore
            columns={"Year": "year", "Weighted average": "cost"}, errors="raise"
        )
    )
    onshore_wind["technology"] = "Onshore wind"

    # Concentrated solar power.
    csp = data.parse("Fig 5.7", skiprows=4).dropna(how="all", axis=1)  # type: ignore
    latest_year = csp.columns[-1]
    csp = (
        csp[csp[f"{latest_year} USD/kWh"] == "Weighted average"]
        .melt(id_vars=f"{latest_year} USD/kWh", var_name="year", value_name="cost")[["year", "cost"]]
        .reset_index(drop=True)
    )
    csp["technology"] = "Concentrated solar power"

    # Offshore wind.
    offshore_wind = data.parse("Fig 4.12", skiprows=3).rename(  # type: ignore
        columns={"Year": "year", "Weighted average": "cost"}, errors="raise"
    )[["year", "cost"]]
    offshore_wind["technology"] = "Offshore wind"

    # Geothermal.
    geothermal = data.parse("Fig 7.4", skiprows=5).rename(
        columns={"Year": "year", "Weighted average": "cost"}, errors="raise"
    )[["year", "cost"]]  # type: ignore
    geothermal["technology"] = "Geothermal"

    # Bioenergy.
    bioenergy = (
        data.parse("Fig 8.1", skiprows=20)
        .dropna(axis=1, how="all")
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")  # type: ignore
    )
    bioenergy = bioenergy[bioenergy["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
    bioenergy["technology"] = "Bioenergy"

    # Hydropower.
    hydropower = (
        data.parse("Fig 6.1", skiprows=20)
        .dropna(how="all", axis=1)
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")  # type: ignore
    )
    hydropower = hydropower[hydropower["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
    hydropower["technology"] = "Hydropower"

    # Concatenate all sources into one table.
    tb = pr.concat([solar_pv, onshore_wind, csp, offshore_wind, geothermal, bioenergy, hydropower], ignore_index=True)

    # Add country column.
    tb["country"] = "World"

    return tb


def extract_country_cost_from_excel_file(data: pr.ExcelFile) -> Table:
    """Extract weighted-average LCOE of certain countries and certain energy sources from the excel file.

    Only onshore wind and solar photovoltaic seem to have this data, and only for specific countries.

    Parameters
    ----------
    data : pr.ExcelFile
        Raw data.

    Returns
    -------
    tb : Table
        LCOE for different energy sources.
    """
    # Extract LCOE for specific countries and technologies (those that are available in original data).

    # Solar photovoltaic.
    solar_pv = (
        data.parse("Fig. 3.11", skiprows=5, usecols=lambda column: "Unnamed" not in column)
        .dropna(how="all", axis=1)
        .rename(columns={"Country": "country", "Year": "year"}, errors="raise")
    )
    solar_pv = solar_pv.rename(
        columns={column: "cost" for column in solar_pv.columns if "Weighted" in column}, errors="raise"
    ).drop(columns=[column for column in solar_pv.columns if "Percentage" in column], errors="raise")

    # Onshore wind.
    onshore_wind = (
        data.parse("Fig 2.12", skiprows=6)
        .dropna(how="all", axis=1)
        .rename(columns={"Country": "country"}, errors="raise")
    )

    # Country column is repeated. Drop it, and drop column of percentage decrease.
    onshore_wind = onshore_wind.drop(columns=["Country.1", "% decrease "], errors="raise")

    # Change to long format.
    onshore_wind = onshore_wind.melt(id_vars="country", var_name="year", value_name="cost")

    # There is data for some additional countries in a separate sheet for smaller markets.
    onshore_wind_extra = (
        data.parse("Fig 2.13", skiprows=6)
        .dropna(how="all", axis=1)
        .rename(columns={"Country": "country", "Year": "year", "Weighted average": "cost"}, errors="raise")
    )

    # Combine onshore wind data.
    onshore_wind_combined = pr.concat([onshore_wind, onshore_wind_extra], ignore_index=True)

    # Add a technology column and concatenate different technologies.
    solar_pv["technology"] = "Solar photovoltaic"
    onshore_wind_combined["technology"] = "Onshore wind"
    combined = pr.concat([solar_pv, onshore_wind_combined], ignore_index=True)

    return combined


def combine_global_and_national_data(tb_costs_global: Table, tb_costs_national: Table) -> Table:
    # Combine global and national data.
    tb_combined = pr.concat([tb_costs_global, tb_costs_national], ignore_index=True).astype({"year": int})

    # Convert from long to wide format.
    tb_combined = tb_combined.pivot(index=["country", "year"], columns="technology", values="cost").reset_index()

    # Remove name of dummy index.
    tb_combined.columns.names = [None]

    # Underscore column names.
    tb_combined = tb_combined.underscore()

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return tb_combined


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("renewable_power_generation_costs.xlsx")
    data = snap.ExcelFile()

    # Extract global, weighted-average LCOE cost for all energy sources.
    tb_costs_global = extract_global_cost_for_all_sources_from_excel_file(data=data)

    # Extract national LCOE for specific countries and technologies.
    tb_costs_national = extract_country_cost_from_excel_file(data=data)

    # Combine global and national data.
    tb_combined = combine_global_and_national_data(tb_costs_global=tb_costs_global, tb_costs_national=tb_costs_national)

    # Extract global data on solar photovoltaic module prices.
    tb_solar_pv_prices = prepare_solar_pv_module_prices(data=data)

    #
    # Save outputs.
    #
    # Create a new Meadow dataset and reuse walden metadata.
    ds = create_dataset(
        dest_dir=dest_dir,
        tables=[tb_combined, tb_solar_pv_prices],
        default_metadata=snap.metadata,
        check_variables_metadata=True,
    )
    ds.save()

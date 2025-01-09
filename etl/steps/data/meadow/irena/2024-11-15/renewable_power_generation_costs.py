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

# Expected USD year.
# NOTE: We could get this from the version, but, if later on we create a minor upgrade with a different year, this will fail.
#  So, instead, hardcode the year and change it on next update.
EXPECTED_DOLLAR_YEAR = 2023
# Expected unit to be found in each of the LCOE sheets.
EXPECTED_LCOE_UNIT = f"{EXPECTED_DOLLAR_YEAR} USD/kWh"
# Expected unit to be found in the solar PV module prices sheet.
EXPECTED_SOLAR_PV_MODULE_COST_UNIT = f"{EXPECTED_DOLLAR_YEAR} USD/W"
# Photovoltaic technologies to consider for average monthly PV module costs.
PV_TECHNOLOGIES = [
    # "Crystalline Europe (Germany)",
    # "Crystalline China",
    # "Crystalline Japan",
    # "Thin film a-Si",
    # "Thin film CdS/CdTe",
    "Thin film a-Si/u-Si or Global Price Index (from Q4 2013)",
    # "Bifacial",
    # "High Efficiency",
    # "All black",
    # "Mainstream",
    # "Low Cost",
]


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
    # NOTE: The currency is not explicitly given in sheet 3.2. But it is in sheet B3.1a (we assume it's the same).
    error = "Cost unit for solar PV module prices has changed."
    assert (
        data.parse(sheet_name="Fig B3.1a", skiprows=6).dropna(axis=1).columns[0] == EXPECTED_SOLAR_PV_MODULE_COST_UNIT
    ), error

    # Load upper table in sheet from Figure 3.2, which is:
    # Average monthly solar PV module prices by technology and manufacturing country sold in Europe, 2010 to 2021.
    pv_prices = data.parse(sheet_name="Fig 3.2", skiprows=7).dropna(axis=1, how="all")
    error = "The file format for solar PV module prices has changed."
    assert pv_prices.columns[0] == "Technology", error

    # Transpose table so that each row corresponds to a month.
    pv_prices = pv_prices.rename(columns={"Technology": "technology"}, errors="raise").melt(
        id_vars="technology", var_name="month", value_name="cost"
    )

    # Select PV technologies.
    error = "Names of solar PV module technologies have changed."
    assert set(PV_TECHNOLOGIES) <= set(pv_prices["technology"]), error
    pv_prices = pv_prices[pv_prices["technology"].isin(PV_TECHNOLOGIES)].reset_index(drop=True)

    # Get month and year from dates.
    pv_prices["year"] = pd.to_datetime(pv_prices["month"], format="%b %y").dt.year
    pv_prices["n_month"] = pd.to_datetime(pv_prices["month"], format="%b %y").dt.month

    # For each year get the average cost over all months.
    pv_prices = (
        pv_prices.groupby(["year"])
        .agg({"cost": "mean", "n_month": "nunique"})
        .rename(columns={"n_month": "n_months"})
        .reset_index()
    )

    # Add column for region.
    pv_prices = pv_prices.assign(**{"country": "World"})

    # Sanity check.
    error = "Incomplete years (with less than 12 months of data) were expected to be either the first or the last."
    assert pv_prices[pv_prices["n_months"] != 12].index.isin([0, len(pv_prices) - 1]).all(), error

    # Ignore years for which we don't have 12 months.
    pv_prices = pv_prices[pv_prices["n_months"] == 12].drop(columns=["n_months"], errors="raise").reset_index(drop=True)

    # Improve table formatting.
    pv_prices = pv_prices.format(sort_columns=True, short_name="solar_photovoltaic_module_prices")

    # Add units.
    pv_prices["cost"].metadata.unit = f"constant {EXPECTED_DOLLAR_YEAR} US$ per watt"
    pv_prices["cost"].metadata.short_unit = "$/W"
    pv_prices[
        "cost"
    ].metadata.description_short = "This data is expressed in US dollars per watt, adjusted for inflation."
    pv_technologies = ", ".join([f"'{tech}'" for tech in PV_TECHNOLOGIES])
    pv_prices["cost"].metadata.description_key = [
        f"IRENA presents solar photovoltaic module prices for a number of different technologies. Here we use the average yearly price for technologies {pv_technologies}."
    ]

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
    error = "The file format for solar PV LCOE has changed."
    assert data.parse("Fig 3.1", skiprows=21).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    solar_pv = (
        data.parse("Fig 3.1", skiprows=22)
        .dropna(how="all", axis=1)
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")
    )
    solar_pv = solar_pv[solar_pv["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
    solar_pv["technology"] = "Solar photovoltaic"

    # Onshore wind.
    error = "The file format for onshore wind LCOE has changed."
    # NOTE: Sheet 2.1 contains LCOE only from 2010, whereas 2.11 contains LCOE from 1984.
    assert data.parse("Fig 2.11", skiprows=2).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    onshore_wind = (
        data.parse("Fig 2.11", skiprows=3)
        .drop(columns="Unnamed: 0", errors="raise")
        .rename(  # type: ignore
            columns={"Year": "year", "Weighted average": "cost"}, errors="raise"
        )
    )
    onshore_wind["technology"] = "Onshore wind"

    # Concentrated solar power.
    error = "The file format for CSP LCOE has changed."
    assert data.parse("Fig 5.1", skiprows=19).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    csp = (
        data.parse("Fig 5.1", skiprows=20)
        .dropna(how="all", axis=1)
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")
    )
    csp = csp[csp["temp"] == "Weighted average"].melt(id_vars="temp", var_name="year", value_name="cost")[
        ["year", "cost"]
    ]
    csp["technology"] = "Concentrated solar power"

    # Offshore wind.
    error = "The file format for offshore wind LCOE has changed."
    assert data.parse("Fig 4.11", skiprows=1).columns[1] == EXPECTED_LCOE_UNIT, error
    offshore_wind = data.parse("Fig 4.11", skiprows=3).rename(  # type: ignore
        columns={"Year": "year", "Weighted average": "cost"}, errors="raise"
    )[["year", "cost"]]
    offshore_wind["technology"] = "Offshore wind"

    # Geothermal.
    # NOTE: Sheet 8.1 contains LCOE only from 2010, whereas 8.4 contains LCOE from 2007.
    error = "The file format for geothermal LCOE has changed."
    assert data.parse("Fig 8.4", skiprows=3).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    geothermal = data.parse("Fig 8.4", skiprows=5).rename(
        columns={"Year": "year", "Weighted average": "cost"}, errors="raise"
    )[["year", "cost"]]  # type: ignore
    geothermal["technology"] = "Geothermal"

    # Bioenergy.
    error = "The file format for bioenergy LCOE has changed."
    assert data.parse("Fig 9.1", skiprows=19).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    bioenergy = (
        data.parse("Fig 9.1", skiprows=20)
        .dropna(axis=1, how="all")
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")  # type: ignore
    )
    bioenergy = bioenergy[bioenergy["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
    bioenergy["technology"] = "Bioenergy"

    # Hydropower.
    error = "The file format for hydropower LCOE has changed."
    assert data.parse("Fig 7.1", skiprows=19).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    hydropower = (
        data.parse("Fig 7.1", skiprows=20)
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
    # NOTE: For some reason, sheet 3.11 contains LCOE from 2010 to 2023 for 15 countries, and 3.12 contains LCOE from 2018 to 2023 for 19 countries.
    #  So, let's take both, check that they are consistent, and concatenate them.
    solar_pv = data.parse("Fig. 3.11", skiprows=5).dropna(how="all", axis=1)
    solar_pv = solar_pv.rename(columns={solar_pv.columns[0]: "country"}, errors="raise").melt(
        id_vars="country", var_name="year", value_name="cost"
    )
    # Keep only rows of LCOE, and drop year changes and empty rows.
    solar_pv = solar_pv[~solar_pv["year"].astype(str).str.startswith("%")].dropna().reset_index(drop=True)

    # Load additional data.
    solar_pv_extra = data.parse("Fig. 3.12", skiprows=8)
    # Drop empty columns and unnecessary regions column.
    solar_pv_extra = solar_pv_extra.drop(
        columns=[column for column in solar_pv_extra.columns if "Unnamed" in str(column)], errors="raise"
    ).drop(columns="Region", errors="raise")
    solar_pv_extra = solar_pv_extra.rename(columns={"Country": "country"}, errors="raise").melt(
        id_vars="country", var_name="year", value_name="cost"
    )

    # Check that, where both tables overlap, they are consistent.
    error = "Expected coincident country-years to have the same LCOE in sheets 3.11 and 3.12."
    check = solar_pv.merge(solar_pv_extra, on=["country", "year"], how="inner")
    # NOTE: Consider relaxing this to coincide within a certain tolerance, if this fails.
    assert (check["cost_x"] == check["cost_y"]).all(), error
    # Concatenate both tables and drop duplicates and empty rows.
    solar_pv = (
        pr.concat([solar_pv, solar_pv_extra], ignore_index=True)
        .drop_duplicates(subset=["country", "year"])
        .dropna()
        .reset_index(drop=True)
    )

    # Onshore wind.
    # NOTE: There is country-level LCOE data in sheets 2.12 and 2.13 (for smaller markets).
    #  Fetch both and concatenate them.
    error = "The file format for onshore wind LCOE has changed."
    assert data.parse("Fig 2.12", skiprows=3).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    # NOTE: Column "Country" appears twice, so drop one of them.
    onshore_wind = (
        data.parse("Fig 2.12", skiprows=6)
        .dropna(how="all", axis=1)
        .drop(columns=["Country.1"])
        .rename(columns={"Country": "country"}, errors="raise")
        .melt(id_vars="country", var_name="year", value_name="cost")
    )
    # Keep only rows of LCOE, and drop year changes and empty rows.
    onshore_wind = onshore_wind[~onshore_wind["year"].astype(str).str.startswith("%")].dropna().reset_index(drop=True)

    error = "The file format for country-level onshore wind LCOE for smaller markets has changed."
    assert data.parse("Fig 2.13", skiprows=3).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    onshore_wind_extra = (
        data.parse("Fig 2.13", skiprows=6)
        .dropna(how="all", axis=1)
        .rename(columns={"Country": "country"}, errors="raise")
        .melt(id_vars="country", var_name="year", value_name="cost")
        .dropna()
        .reset_index(drop=True)
    )

    # Check that there is no overlap between countries in the two tables.
    # NOTE: If there is, then change this to check that the values on coincident country-years are consistent.
    error = "Expected no overlap between countries in sheets 2.12 and 2.13."
    assert set(onshore_wind["country"]).isdisjoint(set(onshore_wind_extra["country"])), error

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
    tb_combined = tb_combined.pivot(
        index=["country", "year"], columns="technology", values="cost", join_column_levels_with="_"
    )

    # Improve table format.
    tb_combined = tb_combined.format(sort_columns=True)

    # Add units.
    for column in tb_combined.columns:
        tb_combined[column].metadata.unit = f"constant {EXPECTED_DOLLAR_YEAR} US$ per kilowatt-hour"
        tb_combined[column].metadata.short_unit = "$/kWh"
        tb_combined[
            column
        ].metadata.description_short = "This data is expressed in US dollars per kilowatt-hour. It is adjusted for inflation but does not account for differences in living costs between countries."

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
    # NOTE: For convenience, we will also add units and a short description here (instead of in the garden step).
    tb_combined = combine_global_and_national_data(tb_costs_global=tb_costs_global, tb_costs_national=tb_costs_national)

    # Extract global data on solar photovoltaic module prices.
    # NOTE: For convenience, we will also add units and a short description here (instead of in the garden step).
    tb_solar_pv_prices = prepare_solar_pv_module_prices(data=data)

    #
    # Save outputs.
    #
    # Create a new Meadow dataset.
    ds = create_dataset(
        dest_dir=dest_dir,
        tables=[tb_combined, tb_solar_pv_prices],
        default_metadata=snap.metadata,
        check_variables_metadata=True,
    )
    ds.save()

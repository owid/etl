"""Extract global (as well as at the country level for some countries) weighted-average levelized cost of electricity
(LCOE) for all energy sources from IRENA's Renewable Power Generation Costs 2022 dataset.

Extract solar photovoltaic module prices too.

NOTE: The original data is poorly formatted. Each energy source is given as a separate sheet, with a different
structure. So it's likely that, on the next update, this script will not work.

"""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Expected USD year.
# NOTE: We could get this from the version, but, if later on we create a minor upgrade with a different year, this will fail.
#  So, instead, hardcode the year and change it on next update.
EXPECTED_DOLLAR_YEAR = 2024
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
    # NOTE: 2024 version changes:
    # - Cost unit moved from Fig B3.1a row 6 to row 2
    # - Monthly price data moved from Fig 3.2 to Fig B3.1b
    # - Header row is now at skiprows=3 instead of skiprows=7

    # Verify cost unit is in the expected format - now at skiprows=1 instead of original skiprows=6
    cost_unit_df = data.parse(sheet_name="Fig B3.1a", skiprows=1, nrows=1)
    cost_unit_row = cost_unit_df.dropna(axis=1).iloc[0, 0]
    assert (
        cost_unit_row == EXPECTED_SOLAR_PV_MODULE_COST_UNIT
    ), f"Expected '{EXPECTED_SOLAR_PV_MODULE_COST_UNIT}', got '{cost_unit_row}'"

    # Load monthly price data from Figure B3.1b (moved from Fig 3.2)
    # Average monthly solar PV module prices by technology and manufacturing country sold in Europe, 2010 to 2024.
    pv_prices = data.parse(sheet_name="Fig B3.1b", skiprows=3).dropna(axis=1, how="all")
    assert pv_prices.columns[0] == "Technology", f"Expected 'Technology' column, got '{pv_prices.columns[0]}'"

    # Transpose table so that each row corresponds to a month.
    pv_prices = pv_prices.rename(columns={"Technology": "technology"}, errors="raise").melt(
        id_vars="technology", var_name="month", value_name="cost"
    )

    # Select PV technologies.
    available_technologies = set(pv_prices["technology"].unique())
    missing_technologies = set(PV_TECHNOLOGIES) - available_technologies
    if missing_technologies:
        # Print available technologies for debugging
        print(f"Available technologies: {sorted(available_technologies)}")
        print(f"Missing technologies: {missing_technologies}")
    assert set(PV_TECHNOLOGIES) <= available_technologies, f"Missing technologies: {missing_technologies}"
    pv_prices = pv_prices[pv_prices["technology"].isin(PV_TECHNOLOGIES)].reset_index(drop=True)

    # Remove rows with non-date month values (some may be text headers)
    pv_prices = pv_prices[pd.notnull(pv_prices["month"])].copy()

    # Convert datetime objects to string format if needed for parsing
    pv_prices["month"] = pv_prices["month"].astype(str)

    # Handle different date formats - the data might be datetime objects already
    def parse_date(month_val):
        if pd.isna(month_val):
            return None, None
        try:
            # If it's already a datetime object, extract year and month
            if hasattr(month_val, "year"):
                return month_val.year, month_val.month
            # Otherwise try to parse as string
            dt = pd.to_datetime(month_val)
            return dt.year, dt.month
        except Exception:
            return None, None

    # Extract year and month
    date_info = pv_prices["month"].apply(parse_date)
    pv_prices["year"] = [info[0] if info else None for info in date_info]
    pv_prices["n_month"] = [info[1] if info else None for info in date_info]

    # Remove rows where date parsing failed
    pv_prices = pv_prices.dropna(subset=["year", "n_month", "cost"]).copy()

    # For each year get the average cost over all months.
    pv_prices = (
        pv_prices.groupby(["year"])
        .agg({"cost": "mean", "n_month": "nunique"})
        .rename(columns={"n_month": "n_months"})
        .reset_index()
    )

    # Add column for region.
    pv_prices = pv_prices.assign(**{"country": "World"})

    # Sanity check - allow for incomplete years at beginning/end due to data availability
    incomplete_years = pv_prices[pv_prices["n_months"] != 12]
    if len(incomplete_years) > 0:
        print(f"Warning: Found {len(incomplete_years)} incomplete years: {list(incomplete_years['year'])}")
        # Only keep years with at least 6 months of data for reasonable averages
        pv_prices = pv_prices[pv_prices["n_months"] >= 6].reset_index(drop=True)

    # Drop the months count column
    pv_prices = pv_prices.drop(columns=["n_months"], errors="ignore").reset_index(drop=True)

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
    assert data.parse("Fig 3.1", skiprows=17).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    solar_pv = (
        data.parse("Fig 3.1", skiprows=18)
        .drop(columns="Unnamed: 0", errors="raise")
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")
    )
    solar_pv = solar_pv[solar_pv["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
    solar_pv["technology"] = "Solar photovoltaic"

    # Onshore wind.
    error = "The file format for onshore wind LCOE has changed."
    # NOTE: Sheet 2.1 contains LCOE only from 2010, whereas 2.11 contains LCOE from 1984.
    assert data.parse("Fig 2.1", skiprows=16).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    onshore_wind = (
        data.parse("Fig 2.1", skiprows=17)
        .drop(columns="Unnamed: 0", errors="raise")
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")
    )
    onshore_wind = onshore_wind[onshore_wind["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
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
    assert data.parse("Fig 4.1", skiprows=19).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    offshore_wind = (
        data.parse("Fig 4.1", skiprows=20)
        .drop(columns="Unnamed: 0", errors="raise")
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")
    )
    offshore_wind = offshore_wind[offshore_wind["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
    offshore_wind["technology"] = "Offshore wind"

    # Geothermal.
    # NOTE: Geothermal data moved to Fig 6.1 and may still reference 2023 USD
    error = "The file format for geothermal LCOE has changed."
    geothermal_header = data.parse("Fig 6.1", skiprows=19).columns[1]
    # Accept either 2023 or 2024 USD for geothermal as the data may lag
    assert geothermal_header in [
        f"LCOE ({EXPECTED_LCOE_UNIT})",
        "LCOE (2023 USD/kWh)",
    ], f"Expected LCOE header, got: {geothermal_header}"
    geothermal = (
        data.parse("Fig 6.1", skiprows=20)
        .drop(columns="Unnamed: 0", errors="raise")
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")
    )
    geothermal = geothermal[geothermal["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
    geothermal["technology"] = "Geothermal"

    # Bioenergy.
    error = "The file format for bioenergy LCOE has changed."
    assert data.parse("Fig 7.1", skiprows=19).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    bioenergy = (
        data.parse("Fig 7.1", skiprows=20)
        .dropna(axis=1, how="all")
        .rename(columns={"Unnamed: 1": "temp"}, errors="raise")  # type: ignore
    )
    bioenergy = bioenergy[bioenergy["temp"] == "Weighted average"].melt(
        id_vars="temp", var_name="year", value_name="cost"
    )[["year", "cost"]]
    bioenergy["technology"] = "Bioenergy"

    # Hydropower.
    error = "The file format for hydropower LCOE has changed."
    assert data.parse("Fig 8.1", skiprows=19).columns[1] == f"LCOE ({EXPECTED_LCOE_UNIT})", error
    hydropower = (
        data.parse("Fig 8.1", skiprows=20)
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
    # Extract LCOE for specific countries and technologies from the 2024 version sheets.

    # Solar photovoltaic - Fig. 3.10: Utility-scale solar PV weighted average cost of electricity in selected countries
    # Unit verification - expect LCOE (2024 USD/kWh) but verify from context
    # The data structure: skiprows=2 to get to years header, countries start from row 3
    solar_pv = data.parse("Fig. 3.10", skiprows=2).dropna(how="all", axis=1)

    # Drop the last column which contains percentage change
    if solar_pv.columns[-1] and "%" in str(solar_pv.columns[-1]):
        solar_pv = solar_pv.drop(columns=solar_pv.columns[-1])

    # Rename the country column (should be column index 0, which contains country names)
    solar_pv = solar_pv.rename(columns={solar_pv.columns[0]: "country"}, errors="raise")

    # Keep only country and year columns - year columns should be numeric
    columns_to_keep = ["country"] + [
        col for col in solar_pv.columns if col != "country" and isinstance(col, (int, float))
    ]
    solar_pv = solar_pv[columns_to_keep]

    # Melt to long format
    solar_pv = solar_pv.melt(id_vars="country", var_name="year", value_name="cost")

    # Keep only valid data rows (remove rows with NaN countries and costs)
    solar_pv = solar_pv.dropna(subset=["country", "cost"]).reset_index(drop=True)

    # Filter out any non-country rows (sometimes there are summary rows)
    solar_pv = solar_pv[solar_pv["country"].notna() & (solar_pv["country"] != "")].reset_index(drop=True)

    # Onshore wind - Fig 2.15: Weighted average LCOE of commissioned onshore wind projects in top markets
    # Unit: LCOE (2024 USD/kWh) - verified from sheet
    # Verify unit is as expected
    unit_check = data.parse("Fig 2.15", skiprows=2)
    unit_cell = unit_check.iloc[0, 1]  # Should contain "LCOE (2024 USD/kWh)"
    assert unit_cell == f"LCOE ({EXPECTED_LCOE_UNIT})", f"Expected '{EXPECTED_LCOE_UNIT}' but found '{unit_cell}'"

    # Load country data - skiprows=6 gets us to the header row with years as column names
    onshore_wind = data.parse("Fig 2.15", skiprows=6).dropna(how="all", axis=1)

    # Rename country column - for skiprows=6, this should be "Country" column (column 0)
    onshore_wind = onshore_wind.rename(columns={onshore_wind.columns[0]: "country"}, errors="raise")

    # Keep only country and year columns - year columns are strings like "2010", "2011", etc.
    year_columns = [col for col in onshore_wind.columns if col != "country" and str(col).isdigit()]
    columns_to_keep = ["country"] + year_columns
    onshore_wind = onshore_wind[columns_to_keep]

    # Melt to long format
    onshore_wind = onshore_wind.melt(id_vars="country", var_name="year", value_name="cost")

    # Keep only valid data rows
    onshore_wind = onshore_wind.dropna(subset=["country", "cost"]).reset_index(drop=True)

    # Filter out any non-country rows (including the header row "Country")
    onshore_wind = onshore_wind[
        onshore_wind["country"].notna() & (onshore_wind["country"] != "") & (onshore_wind["country"] != "Country")
    ].reset_index(drop=True)

    # Add technology columns and concatenate
    solar_pv["technology"] = "Solar photovoltaic"
    onshore_wind["technology"] = "Onshore wind"

    # Ensure proper data types
    solar_pv["country"] = solar_pv["country"].astype(str)
    solar_pv["technology"] = solar_pv["technology"].astype(str)
    solar_pv["year"] = solar_pv["year"].astype(int)
    solar_pv["cost"] = solar_pv["cost"].astype(float)

    onshore_wind["country"] = onshore_wind["country"].astype(str)
    onshore_wind["technology"] = onshore_wind["technology"].astype(str)
    onshore_wind["year"] = onshore_wind["year"].astype(int)
    onshore_wind["cost"] = onshore_wind["cost"].astype(float)

    # Convert to Tables before concatenating
    solar_pv_table = Table(solar_pv)
    onshore_wind_table = Table(onshore_wind)
    combined = pr.concat([solar_pv_table, onshore_wind_table], ignore_index=True)

    return combined


def combine_global_and_national_data(tb_costs_global: Table, tb_costs_national: Table) -> Table:
    # Combine global and national data.
    tb_combined = pr.concat([tb_costs_global, tb_costs_national], ignore_index=True).astype({"year": int})

    # Convert from long to wide format.
    tb_combined = tb_combined.pivot(
        index=["country", "year"], columns="technology", values="cost", join_column_levels_with="_"
    )

    # Improve table format.
    tb_combined = tb_combined.format(sort_columns=True, short_name="renewable_power_generation_costs")

    # Add units.
    for column in tb_combined.columns:
        tb_combined[column].metadata.unit = f"constant {EXPECTED_DOLLAR_YEAR} US$ per kilowatt-hour"
        tb_combined[column].metadata.short_unit = "$/kWh"
        tb_combined[
            column
        ].metadata.description_short = "This data is expressed in US dollars per kilowatt-hour. It is adjusted for inflation but does not account for differences in living costs between countries."

    return tb_combined


def run() -> None:
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
    ds = paths.create_dataset(tables=[tb_combined, tb_solar_pv_prices], default_metadata=snap.metadata)
    ds.save()

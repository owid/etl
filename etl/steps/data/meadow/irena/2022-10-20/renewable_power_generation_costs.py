"""Extract weighted average LCOE for all energy sources from IRENA's Renewable Power Generation Costs 2022 dataset.

"""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from shared import CURRENT_DIR

from etl.helpers import Names
from etl.steps.data.converters import convert_walden_metadata

# Details of input dataset.
WALDEN_VERSION = "2022-10-07"
# Details of output dataset.
VERSION = "2022-10-20"
# Get naming conventions.
N = Names(str(CURRENT_DIR / "renewable_power_generation_costs"))


# +
# It's unclear if this data will be used. If so, it could be a separate table.
# def prepare_pv_data(data_file: str) -> pd.DataFrame:
#     """Prepare yearly data on solar photovoltaic costs.

#     Monthly data will be averaged, and only complete years (with 12 informed months) will be considered.

#     Parameters
#     ----------
#     data_file : str
#         Path to raw data (IRENA's excel file on renewable power generation costs).

#     Returns
#     -------
#     pv_prices : pd.DataFrame
#         PV prices.

#     """
#     # Photovoltaic technologies to choose for average monthly prices.
#     pv_technologies = ["Thin film a-Si/u-Si or Global Index (from Q4 2013)"]
#     # Load upper table in sheet from Figure 3.2, which is:
#     # Average monthly solar PV module prices by technology and manufacturing country sold in Europe, 2010 to 2021.
#     pv_prices = pd.read_excel(
#         data_file, sheet_name="Fig 3.2", skiprows=4, skipfooter=18, usecols=lambda column: "Unnamed" not in column
#     )

#     # Transpose dataframe so that each row corresponds to a month.
#     pv_prices = pv_prices.rename(columns={"2021 USD/W": "technology"}).melt(
#         id_vars="technology", var_name="month", value_name="cost"
#     )

#     # Select PV technologies.
#     pv_prices = pv_prices[pv_prices["technology"].isin(pv_technologies)].reset_index(drop=True)

#     # Get year from dates.
#     pv_prices["year"] = pd.to_datetime(pv_prices["month"], format="%b %y").dt.year

#     # For each year get the average cost over all months.
#     pv_prices = (
#         pv_prices.groupby(["technology", "year"])
#         .agg({"cost": "mean", "year": "count"})
#         .rename(columns={"year": "n_months"})
#         .reset_index()
#     )

#     # Ignore years for which we don't have 12 months.
#     pv_prices = pv_prices[pv_prices["n_months"] == 12].drop(columns=["n_months"]).reset_index(drop=True)

#     # Set an appropriate index and sort conveniently.
#     pv_prices = pv_prices.set_index(["technology", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

#     return cast(pd.DataFrame, pv_prices)
# -

def extract_cost_for_all_sources_from_excel_file(local_file: str) -> pd.DataFrame:
    # Load file as an excel object.
    excel_object = pd.ExcelFile(local_file)

    # Extract weighted average LCOE for different sources:

    # Solar photovoltaic.
    solar_pv = excel_object.parse("Fig 3.1", skiprows=22).dropna(how="all", axis=1).\
        rename(columns={"Unnamed: 1": "temp"})
    solar_pv = solar_pv[solar_pv["temp"] == "Weighted average"].melt(id_vars="temp", var_name="year", value_name="cost")[["year", "cost"]]
    solar_pv["technology"] = "Solar photovoltaic"

    # Onshore wind.
    onshore_wind = excel_object.\
        parse("Fig 2.12", skiprows=3, usecols=lambda column: "Unnamed" not in column).\
        rename(columns={"Year": "year", "Weighted average": "cost"})
    onshore_wind["technology"] = "Onshore wind"

    # Concentrated solar power.
    csp = excel_object.parse("Fig 5.7", skiprows=4).dropna(how="all", axis=1)
    csp = csp[csp["2021 USD/kWh"]=="Weighted average"].melt(id_vars="2021 USD/kWh", var_name="year", value_name="cost")[["year", "cost"]].reset_index(drop=True)
    csp["technology"] = "Concentrated solar power"

    # Offshore wind.
    offshore_wind = excel_object.parse("Fig 4.13", skiprows=3).\
        rename(columns={"Year": "year", "Weighted average": "cost"})[["year", "cost"]]
    offshore_wind["technology"] = "Offshore wind"

    # Geothermal.
    geothermal = excel_object.parse("Fig 7.4", skiprows=5).\
        rename(columns={"Year": "year", "Weighted average": "cost"})[["year", "cost"]]
    geothermal["technology"] = "Geothermal"

    # Bioenergy.
    bioenergy = excel_object.parse("Fig 8.1", skiprows=20).dropna(axis=1, how="all").\
        rename(columns={"Unnamed: 1": "temp"})
    bioenergy = bioenergy[bioenergy["temp"] =="Weighted average"].\
        melt(id_vars="temp", var_name="year", value_name="cost")[["year", "cost"]]
    bioenergy["technology"] = "Bioenergy"

    # Hydropower.
    hydropower = excel_object.parse("Fig 6.1", skiprows=20).dropna(how="all", axis=1).\
        rename(columns={"Unnamed: 1": "temp"})
    hydropower = hydropower[hydropower["temp"] == "Weighted average"].\
        melt(id_vars="temp", var_name="year", value_name="cost")[["year", "cost"]]
    hydropower["technology"] = "Hydropower"

    # Concatenate all sources into one dataframe.
    df = pd.concat([solar_pv, onshore_wind, csp, offshore_wind, geothermal, bioenergy, hydropower], ignore_index=True)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["technology", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return df


def run(dest_dir: str) -> None:
    # Retrieve raw data from Walden.
    walden_ds = WaldenCatalog().find_one(
        namespace="irena", short_name="renewable_power_generation_costs", version=WALDEN_VERSION
    )
    local_file = walden_ds.ensure_downloaded()
    
    # Extract weighted average LCOE cost for all energy sources.
    df = extract_cost_for_all_sources_from_excel_file(local_file=local_file)

    # Create a new Meadow dataset and reuse walden metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION

    # Create a new table with metadata from Walden.
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)

    # Underscore all table columns.
    tb = underscore_table(tb)

    # Add table to the dataset and save dataset.
    ds.add(tb)
    ds.save()

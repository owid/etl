"""Extract data from IRENA's Renewable Power Generation Costs 2022 dataset.

For the moment, we only extract PV costs.

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

# Photovoltaic technologies to choose for average monthly prices.
PV_TECHNOLOGIES = ["Thin film a-Si/u-Si or Global Index (from Q4 2013)"]


def prepare_pv_data(data_file: str) -> pd.DataFrame:
    """Prepare yearly data on solar photovoltaic costs.

    Monthly data will be averaged, and only complete years (with 12 informed months) will be considered.

    Parameters
    ----------
    data_file : str
        Path to raw data (IRENA's excel file on renewable power generation costs).

    Returns
    -------
    pv_prices : pd.DataFrame
        PV prices.

    """
    # Load upper table in sheet from Figure 3.2, which is:
    # Average monthly solar PV module prices by technology and manufacturing country sold in Europe, 2010 to 2021.
    pv_prices = pd.read_excel(
        data_file, sheet_name="Fig 3.2", skiprows=4, skipfooter=18, usecols=lambda column: "Unnamed" not in column
    )

    # Transpose dataframe so that each row corresponds to a month.
    pv_prices = pv_prices.rename(columns={"2021 USD/W": "technology"}).melt(
        id_vars="technology", var_name="month", value_name="cost"
    )

    # Select PV technologies.
    pv_prices = pv_prices[pv_prices["technology"].isin(PV_TECHNOLOGIES)].reset_index(drop=True)

    # Get year from dates.
    pv_prices["year"] = pd.to_datetime(pv_prices["month"], format="%b %y").dt.year

    # For each year get the average cost over all months.
    pv_prices = (
        pv_prices.groupby(["technology", "year"])
        .agg({"cost": "mean", "year": "count"})
        .rename(columns={"year": "n_months"})
        .reset_index()
    )

    # Ignore years for which we don't have 12 months.
    pv_prices = pv_prices[pv_prices["n_months"] == 12].drop(columns=["n_months"]).reset_index(drop=True)

    # Set an appropriate index and sort conveniently.
    pv_prices = pv_prices.set_index(["technology", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return cast(pd.DataFrame, pv_prices)


def run(dest_dir: str) -> None:
    # Retrieve raw data from Walden.
    walden_ds = WaldenCatalog().find_one(
        namespace="irena", short_name="renewable_power_generation_costs", version=WALDEN_VERSION
    )
    local_file = walden_ds.ensure_downloaded()

    # Load and prepare data on photovoltaic costs.
    df = prepare_pv_data(data_file=local_file)

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

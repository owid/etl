"""Combine UK BEIS' historical electricity with our electricity mix dataset (by BP & Ember) to obtain a long-run
electricity mix in the UK.

"""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils import dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_electricity_mix_data(df_elec: pd.DataFrame) -> pd.DataFrame:
    """Select necessary columns from the electricity mix, and select rows corresponding to the UK.

    Parameters
    ----------
    df_elec : pd.DataFrame
        Data from the main table of the electricity mix dataset.

    Returns
    -------
    df_elec : pd.DataFrame
        Selected columns and rows from the electricity mix data.

    """
    df_elec = df_elec.copy()

    # Select columns and rename them conveniently.
    elec_columns = {
        "country": "country",
        "year": "year",
        "coal_generation__twh": "coal_generation",
        "gas_generation__twh": "gas_generation",
        "oil_generation__twh": "oil_generation",
        "hydro_generation__twh": "hydro_generation",
        "nuclear_generation__twh": "nuclear_generation",
        "other_renewables_including_bioenergy_generation__twh": "other_renewables_generation",
        "solar_generation__twh": "solar_generation",
        "total_generation__twh": "total_generation",
        "wind_generation__twh": "wind_generation",
        "total_net_imports__twh": "net_imports",
    }

    # Select necessary columns from electricity mix dataset.
    df_elec = df_elec[list(elec_columns)].rename(columns=elec_columns)

    # Select UK data from Ember dataset.
    df_elec = df_elec[df_elec["country"] == "United Kingdom"].reset_index(drop=True)

    return df_elec


def prepare_beis_data(df_beis: pd.DataFrame) -> pd.DataFrame:
    """Select (and rename) columns from the UK historical electricity data from BEIS.

    Parameters
    ----------
    df_beis : pd.DataFrame
        Combined data for UK historical electricity data from BEIS.

    Returns
    -------
    df_beis : pd.DataFrame
        Selected columns from the UK historical electricity data.

    """
    df_beis = df_beis.copy()

    # Select columns and rename them conveniently.
    beis_columns = {
        "country": "country",
        "year": "year",
        "coal": "coal_generation",
        "oil": "oil_generation",
        "electricity_generation": "total_generation",
        "gas": "gas_generation",
        "hydro": "hydro_generation",
        "nuclear": "nuclear_generation",
        "net_imports": "net_imports",
        "implied_efficiency": "implied_efficiency",
        "wind_and_solar": "wind_and_solar_generation",
    }
    df_beis = df_beis[list(beis_columns)].rename(columns=beis_columns)

    return df_beis


def combine_beis_and_electricity_mix_data(df_beis: pd.DataFrame, df_elec: pd.DataFrame) -> pd.DataFrame:
    """Combine BEIS data on UK historical electricity with the electricity mix data (after having selected rows for only
    the UK).

    There are different processing steps done to the data, see comments below in the code.

    Parameters
    ----------
    df_beis : pd.DataFrame
        Selected data from BEIS on UK historical electricity.
    df_elec : pd.DataFrame
        Selected data from the electricity mix (after having selected rows for the UK).

    Returns
    -------
    df_combined : pd.DataFrame
        Combined and processed data with a verified index.

    """
    # In the BEIS dataset, wind and solar are given as one joined variable.
    # Check if we can ignore it (since it's better to have the two sources separately).
    # Find the earliest year informed in the electricity mix for solar or wind generation.
    solar_or_wind_first_year = df_elec[df_elec["wind_generation"].notnull() | df_elec["solar_generation"].notnull()][
        "year"
    ].min()
    # Now check that, prior to that year, all generation from solar and wind was zero.
    assert df_beis[df_beis["year"] < solar_or_wind_first_year]["wind_and_solar_generation"].fillna(0).max() == 0
    # Therefore, since wind and solar is always zero (prior to the beginning of the electricity mix data)
    # we can ignore this column from the BEIS dataset.
    df_beis = df_beis.drop(columns=["wind_and_solar_generation"])
    # And create two columns of zeros for wind and solar.
    df_beis["solar_generation"] = 0
    df_beis["wind_generation"] = 0
    # Similarly, given that in the BEIS dataset there is no data about other renewable sources (apart from hydro, solar
    # and wind), we can assume that the contribution from other renewables is zero.
    df_beis["other_renewables_generation"] = 0
    # And ensure these new columns do not have any values after the electricity mix data begins.
    df_beis.loc[
        df_beis["year"] >= solar_or_wind_first_year,
        ["solar_generation", "wind_generation", "other_renewables_generation"],
    ] = np.nan

    # BEIS data on fuel input gives raw energy, but we want electricity generation (which is less, given the
    # inefficiencies of the process of burning fossil fuels).
    # They also include a variable on "implied efficiency", which they obtain by dividing the input energy by the total
    # electricity generation.
    # We multiply the raw energy by the efficiency to have an estimate of the electricity generated by each fossil fuel.
    # This only affects data prior to the beginning of the electricity mix's data (which is 1965 for renewables and
    # nuclear, and 1985 for the rest).
    for source in ["coal", "oil", "gas"]:
        df_beis[f"{source}_generation"] *= df_beis["implied_efficiency"]

    # Drop other unnecessary columns.
    df_beis = df_beis.drop(columns=["implied_efficiency"])

    # Combine BEIS and electricity mix data.
    df_combined = dataframes.combine_two_overlapping_dataframes(
        df1=df_elec, df2=df_beis, index_columns=["country", "year"]
    )

    # Add an index and sort conveniently.
    df_combined = df_combined.set_index(["country", "year"]).sort_index().sort_index(axis=1)

    return cast(pd.DataFrame, df_combined)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load BEIS dataset.
    ds_beis: Dataset = paths.load_dependency("uk_historical_electricity")
    # Read main table from dataset.
    tb_beis = ds_beis["uk_historical_electricity"]
    # Create a convenient dataframe out of the table.
    df_beis = pd.DataFrame(tb_beis).reset_index()

    # Load electricity mix dataset.
    ds_elec: Dataset = paths.load_dependency("electricity_mix")
    # Read main table from dataset.
    tb_elec = ds_elec["electricity_mix"]
    # Create a convenient dataframe out of the table.
    df_elec = pd.DataFrame(tb_elec).reset_index()

    #
    # Process data.
    #
    # Prepare electricity mix data.
    df_elec = prepare_electricity_mix_data(df_elec=df_elec)

    # Prepare BEIS data.
    df_beis = prepare_beis_data(df_beis=df_beis)

    # Combine BEIS and electricity mix data.
    df_combined = combine_beis_and_electricity_mix_data(df_beis=df_beis, df_elec=df_elec)

    # Create a new table with combined data (and no metadata).
    tb_combined = Table(df_combined, short_name="uk_historical_electricity")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined])
    ds_garden.save()

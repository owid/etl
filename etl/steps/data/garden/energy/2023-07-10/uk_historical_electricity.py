"""Combine UK BEIS' historical electricity with our electricity mix dataset (by BP & Ember) to obtain a long-run
electricity mix in the UK.

"""

import numpy as np
from owid.catalog import Dataset, Table
from owid.datautils import dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_electricity_mix_data(tb_elec: Table) -> Table:
    """Select necessary columns from the electricity mix, and select rows corresponding to the UK.

    Parameters
    ----------
    tb_elec : Table
        Data from the main table of the electricity mix dataset.

    Returns
    -------
    tb_elec : Table
        Selected columns and rows from the electricity mix data.

    """
    tb_elec = tb_elec.copy()

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
    tb_elec = tb_elec[list(elec_columns)].rename(columns=elec_columns)

    # Select UK data from Ember dataset.
    tb_elec = tb_elec[tb_elec["country"] == "United Kingdom"].reset_index(drop=True)

    return tb_elec


def prepare_beis_data(tb_beis: Table) -> Table:
    """Select (and rename) columns from the UK historical electricity data from BEIS.

    Parameters
    ----------
    tb_beis : Table
        Combined data for UK historical electricity data from BEIS.

    Returns
    -------
    tb_beis : Table
        Selected columns from the UK historical electricity data.

    """
    tb_beis = tb_beis.copy()

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
    tb_beis = tb_beis[list(beis_columns)].rename(columns=beis_columns)

    return tb_beis


def combine_beis_and_electricity_mix_data(tb_beis: Table, tb_elec: Table) -> Table:
    """Combine BEIS data on UK historical electricity with the electricity mix data (after having selected rows for only
    the UK).

    There are different processing steps done to the data, see comments below in the code.

    Parameters
    ----------
    tb_beis : Table
        Selected data from BEIS on UK historical electricity.
    tb_elec : Table
        Selected data from the electricity mix (after having selected rows for the UK).

    Returns
    -------
    tb_combined : Table
        Combined and processed data with a verified index.

    """
    # In the BEIS dataset, wind and solar are given as one joined variable.
    # Check if we can ignore it (since it's better to have the two sources separately).
    # Find the earliest year informed in the electricity mix for solar or wind generation.
    solar_or_wind_first_year = tb_elec[tb_elec["wind_generation"].notnull() | tb_elec["solar_generation"].notnull()][
        "year"
    ].min()
    # Now check that, prior to that year, all generation from solar and wind was zero.
    assert tb_beis[tb_beis["year"] < solar_or_wind_first_year]["wind_and_solar_generation"].fillna(0).max() == 0
    # Therefore, since wind and solar is always zero (prior to the beginning of the electricity mix data)
    # we can ignore this column from the BEIS dataset.
    tb_beis = tb_beis.drop(columns=["wind_and_solar_generation"])
    # And create two columns of zeros for wind and solar.
    tb_beis["solar_generation"] = 0
    tb_beis["wind_generation"] = 0
    # Similarly, given that in the BEIS dataset there is no data about other renewable sources (apart from hydro, solar
    # and wind), we can assume that the contribution from other renewables is zero.
    tb_beis["other_renewables_generation"] = 0
    # And ensure these new columns do not have any values after the electricity mix data begins.
    tb_beis.loc[
        tb_beis["year"] >= solar_or_wind_first_year,
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
        tb_beis[f"{source}_generation"] *= tb_beis["implied_efficiency"]

    # Drop other unnecessary columns.
    tb_beis = tb_beis.drop(columns=["implied_efficiency"])

    # Combine BEIS and electricity mix data.
    tb_combined = dataframes.combine_two_overlapping_dataframes(
        df1=tb_elec, df2=tb_beis, index_columns=["country", "year"]
    )

    # Add an index and sort conveniently.
    tb_combined = tb_combined.set_index(["country", "year"]).sort_index().sort_index(axis=1)

    return tb_combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load BEIS dataset and read its main table.
    ds_beis: Dataset = paths.load_dependency("uk_historical_electricity")
    tb_beis = ds_beis["uk_historical_electricity"].reset_index()

    # Load electricity mix dataset and read its main table.
    ds_elec: Dataset = paths.load_dependency("electricity_mix")
    tb_elec = ds_elec["electricity_mix"].reset_index()

    #
    # Process data.
    #
    # Prepare electricity mix data.
    tb_elec = prepare_electricity_mix_data(tb_elec=tb_elec)

    # Prepare BEIS data.
    tb_beis = prepare_beis_data(tb_beis=tb_beis)

    # Combine BEIS and electricity mix data.
    tb_combined = combine_beis_and_electricity_mix_data(tb_beis=tb_beis, tb_elec=tb_elec)

    # Update combined table name.
    tb_combined.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir, tables=[tb_combined], default_metadata=ds_beis.metadata, check_variables_metadata=True
    )
    ds_garden.save()

"""
This code combines the data of three different sources of GDP and GDP per capita:
    - World Bank (WDI), in 2017 PPPs (coverage from 1990 to the most recent year available)
    - Maddison Project Database, in 2011 PPPs (coverage from 1820 to the most recent year available)
    - Maddison Database, in 1990 PPPs (coverage from 1 CE to 2008)

The goal is to have a single dataset with GDP and GDP per capita estimations in the very long run (from 1 CE to the most current data).

The units of the variables are different in each source, so the data is processed by applying the growth of the Maddison Project Database between 1820 and 1990 retroactively to the World Bank data, and the growth of the Maddison Database between 1 to 1820 retroactively to the data already adjusted in the previous step.

The Maddison Project Database is a different project from the Maddison Database: the latter was produced by Angus Maddison, while the former is the continuation of his work after his death. Only the Maddison Database includes estimates from 1 CE to 1820.

"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define constants: variables to process and references years where merge is done.
VAR_LIST = ["gdp", "gdp_per_capita"]
YEAR_WDI_MPD = 1990
YEAR_MPD_MD = 1820

# Define accuracy of data (in tens)
# 6 means that the data is accurate to the 10^6
ACCURACY_GDP = 6
ACCURACY_GDP_PER_CAPITA = 2


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load dataset and tables

    # World Bank WDI
    ds_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_wdi["wdi"].reset_index()

    # Maddison Project Database
    ds_mpd = paths.load_dataset("ggdc_maddison")
    tb_mpd = ds_mpd["maddison_gdp"].reset_index()

    # Maddison Database
    ds_md = paths.load_dataset("maddison_database")
    tb_md = ds_md["maddison_database"].reset_index()

    #
    # Process data.
    tb = process_and_combine_datasets(tb_wdi, tb_mpd, tb_md)

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_and_combine_datasets(tb_wdi: Table, tb_mpd: Table, tb_md: Table) -> Table:
    """
    Process WDI, Maddison Project Database and Maddison Database to create a single dataset with GDP and GDP per capita estimations in the very long run.
    """

    # Sort by year to apply growth with the correct order
    tb_wdi = tb_wdi.sort_values(by=["year"]).reset_index(drop=True)
    tb_mpd = tb_mpd.sort_values(by=["year"]).reset_index(drop=True)
    tb_md = tb_md.sort_values(by=["year"]).reset_index(drop=True)

    #############################
    # FOR WDI
    # Select GDP and GDP pc in international-$ in 2017 prices
    tb_wdi = tb_wdi[["country", "year", "ny_gdp_mktp_pp_kd", "ny_gdp_pcap_pp_kd"]]
    tb_wdi = tb_wdi.rename(columns={"ny_gdp_mktp_pp_kd": "gdp", "ny_gdp_pcap_pp_kd": "gdp_per_capita"})

    # Filter "World" entity
    tb_wdi = tb_wdi[tb_wdi["country"] == "World"]

    # Drop empty World GDP estimations
    tb_wdi = tb_wdi.dropna().reset_index(drop=True)

    #############################
    # FOR MADDISON PROJECT DATABASE
    # Select only "World" entity
    tb_mpd = tb_mpd[tb_mpd["country"] == "World"].reset_index(drop=True)

    # Drop population, as it's not needed
    tb_mpd = tb_mpd.drop(columns=["population"])

    # Filter years until YEAR_WDI_MPD
    tb_mpd = tb_mpd[tb_mpd["year"] <= YEAR_WDI_MPD].reset_index(drop=True)

    #############################
    # FOR MADDISON DATABASE
    # Keep data until YEAR_MPD_MD
    tb_md = tb_md[tb_md["year"] <= YEAR_MPD_MD].reset_index(drop=True)

    # Drop population, as it's not needed
    tb_md = tb_md.drop(columns=["population"])

    #############################

    # Merge both MPD and WDI world estimations in different columns and add suffixes. This will be useful for the next step.
    tb = tb_mpd.merge(tb_wdi, on="year", how="left", suffixes=("_mpd", "_wdi"), short_name="gdp_historical")

    # Apply Maddison Project Database growth retroactively to YEAR_WDI_MPD WDI data
    tb = create_estimations_from_growth(
        tb=tb, var_list=VAR_LIST, reference_year=YEAR_WDI_MPD, reference_var_suffix="_mpd", to_adjust_var_suffix="_wdi"
    )

    # Concatenate this with original WDI data (data after YEAR_WDI_MPD)
    tb = pr.concat([tb, tb_wdi[tb_wdi["year"] > YEAR_WDI_MPD]], ignore_index=True)

    # Merge datasets to include Maddison Database, which will be used as reference for the next step
    tb = tb.merge(tb_md, on="year", how="outer", suffixes=("", "_md"), sort=True)

    # Apply Maddison Database growth retroactively to YEAR_MPD_MD estimations
    tb = create_estimations_from_growth(
        tb=tb, var_list=VAR_LIST, reference_year=YEAR_MPD_MD, reference_var_suffix="_md", to_adjust_var_suffix=""
    )

    # Round variables to address uncertainty on old estimations (previous to 1990)
    tb["gdp"] = tb["gdp"].round(-ACCURACY_GDP).where(tb["year"] < YEAR_WDI_MPD, tb["gdp"])
    tb["gdp_per_capita"] = (
        tb["gdp_per_capita"].round(-ACCURACY_GDP_PER_CAPITA).where(tb["year"] < YEAR_WDI_MPD, tb["gdp_per_capita"])
    )

    return tb


def create_estimations_from_growth(
    tb: Table, var_list: list, reference_year: int, reference_var_suffix: str, to_adjust_var_suffix: str
) -> Table:
    """
    Adjust estimations of variables according to the growth of a reference variable.

    Parameters
    ----------
    tb : Table
        Table that contains both the reference variable (the one the growth is extracted from) and the variable to be adjusted (the one the growth is applied to).
    var_list : list
        List of the variable types to be adjusted. In this project, ["gdp", "gdp_per_capita"]
    reference_year : int
        Reference year from which the growth will be applied retroactively.
    reference_var_suffix : str
        Suffix of the reference variable (the one the growth is extracted from). In this project, "_mpd" or "_md".
    to_adjust_var_suffix : str
        Suffix of the variable to be adjusted (the one the growth is applied to). In this project, "_wdi" or "".

    Returns
    -------
    tb : Table
        Table with the adjusted variables.
    """
    for var in var_list:
        # Get value from the reference variable in the reference year
        reference_value = tb.loc[tb["year"] == reference_year, f"{var}{reference_var_suffix}"].iloc[0]

        # The scalar is the previous value divided by the reference variable. This is the growth that will be applied retroactively to the variable to be adjusted.
        tb[f"{var}_scalar"] = reference_value / tb[f"{var}{reference_var_suffix}"]

        # Get value to be adjusted in the reference year
        to_adjust_value = tb.loc[tb["year"] == reference_year, f"{var}{to_adjust_var_suffix}"].iloc[0]

        # The estimated values are the division between the reference value and the scalars. This is the variable to be adjusted effectively adjusted by the growth of the reference variable.
        tb[f"{var}_estimated"] = to_adjust_value / tb[f"{var}_scalar"]

        # Rename the estimated variables without the suffix
        tb[f"{var}"] = tb[f"{var}{to_adjust_var_suffix}"].fillna(tb[f"{var}_estimated"])

    # Specify "World" entity for each row
    tb["country"] = "World"

    # Keep only new variables
    tb = tb[["country", "year"] + var_list]

    return tb

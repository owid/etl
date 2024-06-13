"""
This code combines the data of two different sources of GDP and GDP per capita:
    - Maddison Project Database, in 2011 PPPs (coverage from 1820 to the most recent year available)
    - Maddison Database, in 1990 PPPs (coverage from 1 CE to 2008)

The goal is to have a single dataset with GDP and GDP per capita estimations in the very long run (from 1 CE to the most current data).

The units of the variables are different in each source, so the data is processed by applying the growth of the Maddison Database between 1 to 1820 retroactively to the data from the Maddison Project Database.

The Maddison Database is a different project from Maddison Project Database: the former was produced by Angus Maddison, while the latter is the continuation of his work after his death. Only the Maddison Database includes estimates from 1 CE to 1820.

"""

import owid.catalog.processing as pr
from owid.catalog import Table, warnings

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define constants: variables to process and references years where merge is done.
VAR_LIST = ["gdp", "gdp_per_capita"]
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

    # Maddison Project Database
    ds_mpd = paths.load_dataset("maddison_project_database")
    tb_mpd = ds_mpd["maddison_project_database"].reset_index()

    # Maddison Database
    ds_md = paths.load_dataset("maddison_database")
    tb_md = ds_md["maddison_database"].reset_index()

    #
    # Process data.

    tb = process_and_combine_datasets(tb_mpd=tb_mpd, tb_md=tb_md)

    tb = tb.format()

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


def process_and_combine_datasets(tb_mpd: Table, tb_md: Table) -> Table:
    """
    Process Maddison Project Database and Maddison Database to create a single dataset with GDP and GDP per capita estimations in the very long run.
    """

    # Sort by year to apply growth with the correct order
    tb_mpd = tb_mpd.sort_values(by=["year"]).reset_index(drop=True)
    tb_md = tb_md.sort_values(by=["year"]).reset_index(drop=True)

    #############################
    # FOR MADDISON PROJECT DATABASE
    # Select only "World" entity
    tb_mpd = tb_mpd[tb_mpd["country"] == "World"].reset_index(drop=True)

    # Drop population, as it's not needed
    tb_mpd = tb_mpd.drop(columns=["population"])

    #############################
    # FOR MADDISON DATABASE
    # Keep data until YEAR_MPD_MD
    tb_md = tb_md[tb_md["year"] <= YEAR_MPD_MD].reset_index(drop=True)

    # Drop population, as it's not needed
    tb_md = tb_md.drop(columns=["population"])

    #############################

    # Merge datasets to include Maddison Database, which will be used as reference for the next step
    tb = pr.merge(tb_mpd, tb_md, on="year", how="outer", suffixes=("", "_md"), sort=True, short_name="gdp_historical")

    # Apply Maddison Database growth retroactively to YEAR_MPD_MD estimations
    # Remove warnings for different units
    with warnings.ignore_warnings([warnings.DifferentValuesWarning]):
        tb = create_estimations_from_growth(
            tb=tb, var_list=VAR_LIST, reference_year=YEAR_MPD_MD, reference_var_suffix="_md", to_adjust_var_suffix=""
        )

    # Round variables to address uncertainty on old estimations (previous to 1990)
    tb["gdp"] = tb["gdp"].round(-ACCURACY_GDP).where(tb["year"] < YEAR_MPD_MD, tb["gdp"])
    tb["gdp_per_capita"] = (
        tb["gdp_per_capita"].round(-ACCURACY_GDP_PER_CAPITA).where(tb["year"] < YEAR_MPD_MD, tb["gdp_per_capita"])
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
        Suffix of the reference variable (the one the growth is extracted from). In this project, "_md".
    to_adjust_var_suffix : str
        Suffix of the variable to be adjusted (the one the growth is applied to). In this project, "".

    Returns
    -------
    tb : Table
        Table with the adjusted variables.
    """
    for var in var_list:
        # Get value from the reference variable in the reference year
        reference_value = tb.loc[tb["year"] == reference_year, f"{var}{reference_var_suffix}"].iloc[0]

        # The scalar is the previous value divided by the reference variable. This is the growth that will be applied retroactively to the variable to be adjusted.
        tb[f"{var}_scalar"] = tb[f"{var}{reference_var_suffix}"] / reference_value

        # Get value to be adjusted in the reference year
        to_adjust_value = tb.loc[tb["year"] == reference_year, f"{var}{to_adjust_var_suffix}"].iloc[0]

        # The estimated values are the division between the reference value and the scalars. This is the variable to be adjusted effectively adjusted by the growth of the reference variable.
        tb[f"{var}_estimated"] = to_adjust_value * tb[f"{var}_scalar"]

        # Rename the estimated variables without the suffix
        tb[f"{var}"] = tb[f"{var}{to_adjust_var_suffix}"].fillna(tb[f"{var}_estimated"])

    # Specify "World" entity for each row
    tb["country"] = "World"

    # Keep only new variables
    tb = tb[["country", "year"] + var_list]

    return tb

"""Load a meadow dataset and create a garden dataset."""

import re

import numpy as np
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("who")
    tb = ds_meadow["who"].reset_index()
    tb = tb.rename(columns={"name": "country"})
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # The population is given as 'population (thousands)
    tb["pop"] = tb["pop"].astype(float).multiply(1000)
    tb = calculate_population_with_each_category(tb)
    tb = calculate_population_without_service(tb)
    tb = tb.drop(columns=["pop"])
    tb = tb.set_index(["country", "year", "residence"], verify_integrity=True)
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def drop_erroneous_rows(tb: Table) -> Table:
    """
    The values for Kosovo are poorly formatted and the country name is given as a year. Let's remove those.
    """
    tb = tb[~tb["country"].str.startswith("2")]
    tb = tb[tb["year"].notna()]
    return tb


def calculate_population_with_each_category(tb: Table) -> Table:
    """
    Calculate the population living with each category and also by urban/rural.

    Multiply each column by the 'pop' column and append '_pop' to the column name.

    """
    columns = tb.columns.drop(["country", "year", "pop", "residence"])

    for col in columns:
        print(col)
        tb[f"{col}_pop"] = (tb[col] / 100) * tb["pop"]

    return tb


def clean_values(tb: Table) -> Table:
    """
    Some values are strings that begin with either < or >. These are not valid numbers, so we need to clean them by removing those non-numeric characters.
    Additionally, NAs are represented with - in the data, we need to replace those with NaNs.

    """
    # Replace - with NaNs.
    tb = tb.replace("-", np.nan)

    # Remove < and > from strings.
    tb = tb.applymap(lambda x: re.sub(r"[<>]", "", x) if isinstance(x, str) else x)

    # There is a strange column at the end of each table that is just the row number. We can drop this.

    tb = tb.drop(
        columns=[
            "year_survey_name",
            "pct_urban__of_total_population",
            "total_sl",
            "total_proportion_of_population_using__improved_water_supplies_sl",
            "total_proportion_of_population_using_improved__sanitation_facilities__including_shared__sl",
        ],
        axis=1,
        errors="ignore",  # not all tables have these columns
    )

    return tb


def calculate_hygiene_no_services(tb: Table) -> Table:
    """
    Calculate the proportion of the population with no services.

    """
    tb["hyg_ns"] = 100 - tb["hyg_fac"]
    tb = tb.drop(columns=["hyg_fac"], axis=1)
    return tb


def calculate_population_without_service(tb: Table) -> Table:
    """
    Calculate the population without given services

    """
    # * wat_basal
    # * wat_imp
    # * wat_sm
    # * san_imp
    # * san_sm
    # * hyg_bas

    without_cols = ["wat_basal", "wat_imp", "wat_sm", "san_imp", "san_sm", "hyg_bas"]

    for col in without_cols:
        tb[f"{col}_without"] = 100 - tb[col]
        tb[f"{col}_pop_without"] = (tb[f"{col}_without"] / 100) * tb["pop"]

    return tb

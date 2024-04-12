"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers.geo import add_regions_to_table, harmonize_countries
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year to use for the screening and testing rates.
# Should be the most recent year of complete data.
SCREENING_YEAR = 2023

REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow acute flaccid paralysis dataset.
    ds_meadow = paths.load_dataset("polio_afp")
    # Load historical polio dataset
    ds_historical = paths.load_dataset("polio_historical")
    # Load fasttrack Global Polio Eradication Initiative on circulating vaccine derived polio cases
    snap_cvdpv = paths.load_snapshot("gpei.csv")
    tb_cvdpv = snap_cvdpv.read()
    # Dropping this as the total_cvdpv is also in the polio_afp table and has more historical data
    tb_cvdpv = tb_cvdpv.drop(columns=["total_cvdpv"])
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow["polio_afp"].reset_index()
    tb_hist = ds_historical["polio_historical"].reset_index()
    tb_hist = tb_hist.rename(columns={"cases": "total_cases"})
    # Only need this for data prior to 2001
    tb_hist = tb_hist[tb_hist["year"] < 2001]
    #
    # Process data.
    #
    tb = harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Remove data from before 2001.
    tb = remove_pre_2001_data(tb)
    # Remove values > 100% for "Adequate stool collection".
    tb = clean_adequate_stool_collection(tb)
    # Add total cases
    tb["total_cases"] = tb["wild_poliovirus_cases"] + tb["cvdpv_cases"]
    # Need to deal with overlapping years
    tb = pr.concat([tb, tb_hist], axis=0)
    tb = tb.merge(tb_cvdpv, on=["country", "year"], how="left")
    # Add region aggregates.
    tb_reg = add_regions_to_table(
        tb[
            [
                "country",
                "year",
                "afp_cases",
                "wild_poliovirus_cases",
                "cvdpv_cases",
                "total_cases",
                "cvdpv1",
                "cvdpv2",
                "cvdpv3",
            ]
        ],
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )
    tb_reg = tb_reg[tb_reg["country"].isin(REGIONS)]
    tb = pr.concat([tb, tb_reg], axis=0)
    # Add correction factor to estimate polio cases based on reported cases.
    tb = add_correction_factor(tb)
    tb["estimated_cases"] = tb["total_cases"] * tb["correction_factor"]
    # Add polio surveillance status based on the screening and testing rates.
    tb = add_screening_and_testing(tb)

    tb = tb.set_index(["country", "year"], verify_integrity=True)
    tb.metadata.short_name = "polio"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_screening_and_testing(tb: Table, year=SCREENING_YEAR) -> Table:
    """
    Adds the polio surveillance status based on the screening and testing rates.
    For use in this chart: https://ourworldindata.org/grapher/polio-screening-and-testing

    Parameters:
    - tb: table containing polio surveillance data.
    - year: Specific year to filter the data. If None, uses current year.

    Returns:
    - Modified table with a new column for polio surveillance status.
    """
    # tb["polio_surveillance_status"] = pd.NA
    tb.loc[
        (tb["non_polio_afp_rate"] >= 2.0)
        & (tb["pct_adequate_stool_collection"] >= 80)
        & (tb["year"] == SCREENING_YEAR),
        "polio_surveillance_status",
    ] = "Adequate screening and testing"
    tb.loc[
        (tb["non_polio_afp_rate"] >= 2.0) & (tb["pct_adequate_stool_collection"] < 80) & (tb["year"] == SCREENING_YEAR),
        "polio_surveillance_status",
    ] = "Inadequate testing"
    tb.loc[
        (tb["non_polio_afp_rate"] < 2.0) & (tb["pct_adequate_stool_collection"] >= 80) & (tb["year"] == SCREENING_YEAR),
        "polio_surveillance_status",
    ] = "Inadequate screening"
    tb.loc[
        (tb["non_polio_afp_rate"] < 2.0) & (tb["pct_adequate_stool_collection"] < 80) & (tb["year"] == SCREENING_YEAR),
        "polio_surveillance_status",
    ] = "Inadequate screening and testing"
    # Not sure if this is the best way to handle this, the code fails because this indicator doesn't have origins otherwise
    tb["polio_surveillance_status"].metadata.origins = tb["non_polio_afp_rate"].metadata.origins
    return tb


def add_correction_factor(tb: Table) -> Table:
    """
    Adding the correction factor to estimate polio cases based on reported cases.

    Following Tebbens et al (2011) -https://www.sciencedirect.com/science/article/pii/S0264410X10014957?via%3Dihub

    The correction factor is 7 for all years before 1996.
    The correction factor is 1.11 for all countries when 1996 >= year <= 2000 if the 'non_polio_afp_rate' is < 1 OR 'percent_adequate_stool_collection' < 60, then the correction factor = 7.
    If the 'non_polio_afp_rate' is < 2 OR 'percent_adequate_stool_collection' < 80, then the correction factor = 2. If the 'non_polio_afp_rate' is >= 2 OR 'percent_adequate_stool_collection' >= 80, then the correction factor = 1.11.
    If both 'non_polio_afp_rate' and 'percent_adequate_stool_collection' are missing then the correction factor is 7.

    There are some manual changes we make:

    - Namibia had 'percent_adequate_stool_collection' > 100 in 2011 and 2014 but for other years it's correction factor is 1.11 so we set it as 1.11 for 2011 and 2014.

    - For China 1989-92 we set the correction factor to 1.11 and in Oman in 1988.

    (We set the correction factor as NA for all of 2021 as the values of 'percent_adequate_stool_collection' seemed unreliable in this year.)

    """
    # tb["correction_factor"] = pd.NA
    # Correction factor for years 1996-2000 is 1.11.
    tb.loc[(tb["year"] >= 1996) & (tb["year"] <= 2000), "correction_factor"] = 1.11
    # If the 'non_polio_afp_rate' is < 1 OR 'percent_adequate_stool_collection' < 60, then the correction factor = 7.
    tb.loc[(tb["non_polio_afp_rate"] < 1.0) | (tb["pct_adequate_stool_collection"] < 60), "correction_factor"] = 7.0
    # If the 'non_polio_afp_rate' is < 2 OR 'percent_adequate_stool_collection' < 80, then the correction factor = 2.
    tb.loc[(tb["non_polio_afp_rate"] < 2.0) | (tb["pct_adequate_stool_collection"] < 80), "correction_factor"] = 2.0
    # If the 'non_polio_afp_rate' is >= 2 OR 'percent_adequate_stool_collection' >= 80, then the correction factor = 1.11.
    tb.loc[(tb["non_polio_afp_rate"] >= 2.0) & (tb["pct_adequate_stool_collection"] >= 80), "correction_factor"] = 1.11
    # If both 'non_polio_afp_rate' and 'percent_adequate_stool_collection' are missing then the correction factor is 7.
    tb.loc[(tb["non_polio_afp_rate"].isna()) & (tb["pct_adequate_stool_collection"].isna()), "correction_factor"] = 7.0
    # Correction factor for years before 1996 is 7.
    tb.loc[tb["year"] < 1996, "correction_factor"] = 7.0

    # tb.loc[tb["year"] == 2021, "correction_factor"] = np.nan

    # Namibia had 'percent_adequate_stool_collection' > 100 in 2011 and 2014 but for other years it's correction factor is 1.11 so we set it as 1.11 for 2011 and 2014.
    tb["correction_factor"][(tb["country"] == "Namibia") & (tb["year"].isin([2011, 2014]))] = 1.11
    # For China 1989-92 we set the correction factor to 1.11 and in Oman in 1988.
    tb["correction_factor"][(tb["country"] == "China") & (tb["year"].isin([1989, 1990, 1991, 1992]))] = 1.11
    tb["correction_factor"][(tb["country"] == "Oman") & (tb["year"].isin([1988]))] = 1.11
    # Not sure if this is the best way to handle this, the code fails because this indicator doesn't have origins otherwise
    tb["correction_factor"].metadata.origins = tb["non_polio_afp_rate"].metadata.origins
    return tb


def clean_adequate_stool_collection(tb: Table) -> Table:
    """
    Some values for "Adequate stool collection" are over 100%, we should set these to NA.
    """
    tb["pct_adequate_stool_collection"][tb["pct_adequate_stool_collection"] > 100] = pd.NA
    return tb


def remove_pre_2001_data(tb: Table) -> Table:
    """Remove data from before 2001."""
    tb = tb[tb["year"] >= 2001]
    return tb

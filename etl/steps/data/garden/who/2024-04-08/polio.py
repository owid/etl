"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers.geo import add_regions_to_table, harmonize_countries, list_members_of_region
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
    # Load population data to calculate cases per million population
    ds_population = paths.load_dataset("population")
    tb_population = ds_population["population"].reset_index()
    # Load fasttrack Global Polio Eradication Initiative on circulating vaccine derived polio cases
    snap_cvdpv = paths.load_snapshot("gpei.csv")
    tb_cvdpv = snap_cvdpv.read()
    # Dropping this as the total_cvdpv is also in the polio_afp table and has more historical data
    tb_cvdpv = tb_cvdpv.drop(columns=["total_cvdpv"])
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"].reset_index()
    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow["polio_afp"].reset_index()
    tb_hist = ds_historical["polio_historical"].reset_index()
    tb_hist = tb_hist.rename(columns={"cases": "total_cases"})
    # Only need this for data prior to 2001
    tb_hist = tb_hist[tb_hist["year"] < 2001]

    # Remove data from before 2001.
    tb = remove_pre_2001_data(tb)
    # Remove values > 100% for "Adequate stool collection".
    tb = clean_adequate_stool_collection(tb)
    # Add total cases
    tb["total_cases"] = tb["wild_poliovirus_cases"] + tb["cvdpv_cases"]
    # Need to deal with overlapping years
    tb = pr.concat([tb_hist, tb], axis=0)
    tb = harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
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
    tb = add_screening_and_testing(tb, tb_regions, ds_regions)
    tb = add_cases_per_million(tb, tb_population)
    tb.format(short_name="polio")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_cases_per_million(tb: Table, tb_population: Table) -> Table:
    """
    Add cases per million population for each country, for the columns concerning each type of polio cases.
    """
    tb_population = tb_population[["country", "year", "population"]]
    tb = tb.merge(tb_population, on=["country", "year"], how="left")

    cols_to_divide = [
        "afp_cases",
        "wild_poliovirus_cases",
        "cvdpv_cases",
        "total_cases",
        "estimated_cases",
        "cvdpv1",
        "cvdpv2",
        "cvdpv3",
    ]
    for col in cols_to_divide:
        tb[f"{col}_per_million"] = tb[col] / tb["population"] * 1_000_000

    tb = tb.drop(columns=["population"])
    return tb


def list_of_who_countries(tb_regions: Table, ds_regions: Dataset) -> list:
    """List of countries as defined by WHO."""
    who_countries = []
    who_regions = tb_regions[tb_regions["defined_by"] == "who"]
    for region in who_regions["name"]:
        country_list = list_members_of_region(region=region, ds_regions=ds_regions)
        if not country_list:
            raise ValueError(f"No countries found for region {region}")
        who_countries.extend(country_list)
    return who_countries


def identify_low_risk_countries(tb: Table, tb_regions: Table, ds_regions: Dataset) -> Table:
    # Identify low-risk countries (where the surveillance status can be disregarded)
    # High risk entities are those identified in the table on page 48 in this document: https://polioeradication.org/wp-content/uploads/2022/04/GPSAP-2022-2024-EN.pdf
    higher_risk_entities = [
        "Chad",
        "Democratic Republic of Congo",
        "Ethiopia",
        "Niger",
        "Nigeria",
        "Afghanistan",
        "Pakistan",
        "Somalia",
        "Angola",
        "Burkina Faso",
        "Cameroon",
        "Central African Republic",
        "Guinea",
        "Kenya",
        "Mali",
        "South Sudan",
        "Yemen",
        "Benin",
        "Cote d'Ivoire",
        "Equatorial Guinea",
        "Guinea-Bissau",
        "Madagascar",
        "Mozambique",
        "Togo",
        "Iraq",
        "Sudan",
        "Syria",
        "Myanmar",
        "Papua New Guinea",
        "Philippines",
        "Burundi",
        "Congo",
        "Gabon",
        "Gambia",
        "Ghana",
        "Liberia",
        "Senegal",
        "Sierra Leone",
        "Uganda",
        "Zambia",
        "Djibouti",
        "Egypt",
        "Iran",
        "Libya",
        "Tajikistan",
        "Ukraine",
        "Indonesia",
        "Nepal",
        "Haiti",
        "Laos",
        "China",
        "Eritrea",
        "Malawi",
        "Mauritania",
        "Namibia",
        "Rwanda",
        "Tanzania",
        "Zimbabwe",
        "Lebanon",
        "Bangladesh",
        "India",
        "East Timor",
        "Bolivia",
        "Cambodia",
        "Malaysia",
    ]

    difference = [item for item in higher_risk_entities if item not in tb["country"].unique()]
    assert difference == [], f"Entities in the high-risk list that are not in the dataset: {difference}"

    # Define the condition for which countries are not in high-risk entities
    not_high_risk = ~tb["country"].isin(higher_risk_entities)

    # Define the condition for screening year
    is_screening_year = tb["year"] == SCREENING_YEAR

    # Combine conditions and update 'polio_surveillance_status' for matching rows
    tb.loc[not_high_risk & is_screening_year, "polio_surveillance_status"] = "Low risk"

    return tb


def add_screening_and_testing(tb: Table, tb_regions: Dataset, ds_regions: Dataset) -> Table:
    """
    Adds the polio surveillance status based on the screening and testing rates.
    For use in this chart: https://ourworldindata.org/grapher/polio-screening-and-testing

    Parameters:
    - tb: table containing polio surveillance data.
    - year: Specific year to filter the data. If None, uses current year.

    Returns:
    - Modified table with a new column for polio surveillance status.
    """
    # Ensuring we have all the countries in the WHO regions - even if there isn't other polio data for them
    who_countries = list_of_who_countries(tb_regions, ds_regions)
    who_tb = Table({"country": who_countries, "year": SCREENING_YEAR}).copy_metadata(from_table=tb)
    tb = tb.merge(who_tb, on=["country", "year"], how="outer")

    # Add the polio surveillance status based on the screening and testing rates
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

    tb = identify_low_risk_countries(tb, tb_regions, ds_regions)
    # Not sure if this is the best way to handle this, the code fails because this indicator doesn't have origins otherwise
    tb["polio_surveillance_status"] = tb["polio_surveillance_status"].copy_metadata(tb["non_polio_afp_rate"])
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
    tb.loc[(tb["country"] == "Namibia") & (tb["year"].isin([2011, 2014])), "correction_factor"] = 1.11
    # For China 1989-92 we set the correction factor to 1.11 and in Oman in 1988.
    tb.loc[(tb["country"] == "China") & (tb["year"].isin([1989, 1990, 1991, 1992])), "correction_factor"] = 1.11
    tb.loc[(tb["country"] == "Oman") & (tb["year"].isin([1988])), "correction_factor"] = 1.11
    # Not sure if this is the best way to handle this, the code fails because this indicator doesn't have origins otherwise
    tb["correction_factor"].metadata.origins = tb["non_polio_afp_rate"].metadata.origins
    return tb


def clean_adequate_stool_collection(tb: Table) -> Table:
    """
    Some values for "Adequate stool collection" are over 100%, we should set these to NA.
    """
    tb.loc[tb["pct_adequate_stool_collection"] > 100, "pct_adequate_stool_collection"] = pd.NA
    return tb


def remove_pre_2001_data(tb: Table) -> Table:
    """Remove data from before 2001."""
    tb = tb[tb["year"] >= 2001].reset_index(drop=True)
    return tb

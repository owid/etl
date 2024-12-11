"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COMMENTS_DICT = {
    "(P)": "Population census only.",
    "(H)": "Housing census only.",
    "(1)": "Urban areas only.",
    "(2)": "Enumeration of settled population was in November 1986 and of nomads in February 1987.",
    "(3)": "Population figures compiled from administrative registers.",
    "(4)": "Population figures compiled from administrative registers in combination with other sources of data, such as sample surveys.",
    "(5)": "The population by-censuses for 1986 and 1996 were based on one-in-seven sample of the population, while that for 2006 was based on one-in-ten sample of the population.",
    "(6)": "Enumeration of former Yemen Arab Republic.",
    "(7)": "Enumeration of former Democratic Yemen.",
    "(8)": "Through accession of the German Democratic Republic to the Federal Republic of Germany with effect from 3 October 1990, the two German States have united to form one sovereign State. As from the date of unification, the Federal Republic of Germany acts in the United Nations under the designation 'Germany'.",
    "(9)": "Enumeration of former Federal Republic of Germany.",
    "(10)": "Combined with agricultural census.",
    "(11)": "No formal census conducted. A count of numbers of each family group by name, sex, age and whether permanent or expatriate resident is made on 30 or 31 December each year.",
    "(12)": "A register-based test census was conducted on 5 December 2001 on a sample of 1.2% of the population.",
    "(13)": "Due to the circumstances, the census was conducted again in 2004.",
    "(14)": "Census not carried out on the territory of Kosovo and Metohia.",
    "(15)": "Rolling Census based on continuous sample survey.",
    "(16)": "Census was planned to be conducted using staggered enumerations province by province. At the end of 2014, only 6 of the 34 provinces had been enumerated.",
    "(17)": "Traditional decennial census with full field enumeration, and a continuous sample survey.",
    "(18)": "Population figures compiled from administrative registers and sample surveys while data on housing characteristics are collected through full field enumeration.",
    "(19)": "Cancelled.",
}

MONTHS_DICT = {
    "Jan.": "January",
    "Feb.": "February",
    "Mar.": "March",
    "Apr.": "April",
    "Jun.": "June",
    "Jul.": "July",
    "Aug.": "August",
    "Sep.": "September",
    "Oct.": "October",
    "Nov.": "November",
    "Dec.": "December",
}

MIN_YEAR = 1985
CURR_YEAR = 2024


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("census_dates")

    # Read table from meadow dataset.
    tb = ds_meadow["census_dates"].reset_index()

    # remove timeframes from date
    tb["date"] = tb["date"].apply(lambda x: x.split("-")[1] if "-" in x else x)

    # add comments
    tb["comment"] = tb.apply(lambda x: get_comment(x["date"], x["country"]), axis=1)

    # clean date and country columns
    tb["date"] = tb["date"].apply(clean_date)
    tb["date"] = tb["date"].replace(MONTHS_DICT)
    tb["country"] = tb["country"].apply(clean_country)

    # convert date to datetime
    tb["date_as_year"] = tb["date"].apply(date_as_year)

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb_census = years_since_last_census(tb)
    tb_census = add_uk(tb_census)
    tb_census = tb_census.format(["country", "year"])

    # create indicator that shows if a census was conducted in the last 10 years
    tb_census["recent_census"] = tb_census["years_since_last_census"].apply(
        lambda x: np.nan if np.isnan(x) else 1 if x <= 10 else 0
    )

    tb = tb.format(["country", "date"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_census], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_comment(date: str, country: str) -> str:
    """Get comments from footnotes in date and country column."""
    comment = ""
    if date.startswith("("):
        note = date.split(" ")[0]
        comment += COMMENTS_DICT[note]
    if country.endswith(")"):
        if country.split(" ")[-1] not in ["(Malvinas)"]:
            note = country.split(" ")[-1]
            if len(comment) == 0:
                comment += COMMENTS_DICT[note]
            else:
                comment += ", " + COMMENTS_DICT[note]
    return comment


def clean_date(date: str) -> str:
    """Clean date column."""
    if date.startswith("("):
        return " ".join(date.split(" ")[1:])
    else:
        return date


def clean_country(country: str) -> str:
    """Clean country column."""
    if (country.endswith(")")) and (country.split(" ")[-1] not in ["(Malvinas)"]):
        return " ".join(country.split(" ")[:-1])
    else:
        return country


def date_as_year(date: str) -> int:
    """Extract year from date."""
    if " " in date and date.split(" ")[-1].isdigit():
        return int(date.split(" ")[-1])
    elif "." in date:
        return int(date.split(".")[-1])
    elif date.startswith("[") and date.endswith("]"):
        return int(date[1:-1])
    else:
        return int(date)


def years_since_last_census(tb: Table) -> Table:
    countries = tb["country"].unique()
    years = [int(x) for x in range(1985, 2024)]
    rows = []
    for country in countries:
        country_df = tb[tb["country"] == country].sort_values("date_as_year", ascending=True)
        census_years = country_df["date_as_year"].tolist()
        for year in years:
            prev_census = [x for x in census_years if x <= year]
            if prev_census:
                last_census = max([x for x in census_years if x <= year])
                years_since_last_census = year - last_census
            else:
                last_census = None
                years_since_last_census = None
            rows.append(
                {
                    "country": country,
                    "year": year,
                    "last_census": last_census,
                    "years_since_last_census": years_since_last_census,
                }
            )
    tb_census = Table(pd.DataFrame(rows)).copy_metadata(tb)
    tb_census.m.short_name = "years_since_last_census"

    for col in tb_census.columns:
        tb_census[col].metadata = tb["date"].m
        tb_census[col].metadata.origins = tb["date"].m.origins
    return tb_census


def add_uk(tb_census):
    years = [int(x) for x in range(MIN_YEAR, CURR_YEAR)]
    uk_rows = []
    uk_countries = ["England and Wales", "Scotland", "Northern Ireland"]
    for year in years:
        uk_tb = tb_census[tb_census["country"].isin(uk_countries) & (tb_census["year"] == year)]
        uk_last_census = uk_tb["last_census"].max()
        uk_years_since_last_census = year - uk_last_census
        uk_rows.append(
            {
                "country": "United Kingdom",
                "year": year,
                "last_census": uk_last_census,
                "years_since_last_census": uk_years_since_last_census,
            }
        )
    uk_tb_census = Table(pd.DataFrame(uk_rows)).copy_metadata(tb_census)
    tb_census = pr.concat([tb_census, uk_tb_census], axis=0)
    return tb_census

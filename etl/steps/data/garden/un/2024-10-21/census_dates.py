"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

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
    tb["country"] = tb["country"].apply(clean_country)

    # convert date to datetime
    tb["date_as_datetime"] = pd.to_datetime(tb["date"], errors="coerce")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
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

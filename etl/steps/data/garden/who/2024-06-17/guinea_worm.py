"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_TO_CAT_MAP = {
    1997: "1997-1999",
    1998: "1997-1999",
    1999: "1997-1999",
    2000: "2000s",
    2001: "2000s",
    2002: "2000s",
    2003: "2000s",
    2004: "2000s",
    2005: "2000s",
    2006: "2000s",
    2007: "2000s",
    2008: "2000s",
    2009: "2000s",
    2010: "2010s",
    2011: "2010s",
    2012: "2010s",
    2013: "2010s",
    2014: "2010s",
    2015: "2010s",
    2016: "2010s",
    2017: "2010s",
    2018: "2010s",
    2019: "2010s",
    2020: "2020s",
    2021: "2020s",
    2022: "2020s",
    2023: "2020s",
    2024: "2020s",
    "Pre-certification": "Pre-certification",
    "Endemic": "Endemic",
}

YEAR_CATEGORIES = [
    "1997-1999",
    "2000s",
    "2010s",
    "2020s",
    "Pre-certification",
    "Endemic",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    # fasttrack snapshot (with case numbers)
    snap = Snapshot("fasttrack/2024-06-17/guinea_worm.csv")
    # garden dataset (with certification status of countries)
    ds_garden = paths.load_dataset(channel="garden", short_name="guinea_worm")
    # Read tables
    tb_cases = snap.read_csv()
    tb_cert = ds_garden["guinea_worm"].reset_index()

    #
    # Process data.
    #

    # harmonize countries
    tb_cert = geo.harmonize_countries(df=tb_cert, countries_file=paths.country_mapping_path)
    tb_cases = geo.harmonize_countries(df=tb_cases, countries_file=paths.country_mapping_path)

    # split "year_certified" in two columns:
    # - year_certified: year in which country was certified as disease free (without status messages, Int64 type)
    # - time_frame_certified: time frame in which country was certified as disease free (with status messages, Category type)

    tb_cert["time_frame_certified"] = pd.Categorical(
        tb_cert["year_certified"].map(YEAR_TO_CAT_MAP), categories=YEAR_CATEGORIES, ordered=True
    )
    tb_cert["year_certified"] = pd.to_numeric(tb_cert["year_certified"], errors="coerce").astype("Int64")

    # add year in which country was certified as disease free to all rows
    tb_cert = add_year_certified(tb_cert)

    # add rows for current year
    tb_cert = tb_cert[~(tb_cert["year"] == 2023)]  # data has some empty rows for 2023
    tb = add_current_year(tb_cert, tb_cases, year=2023)

    # fix dtypes
    tb["year_certified"] = tb["year_certified"].astype("Int64")

    # format index
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_year_certified(tb):
    "add year in which country was certified as disease free"
    for idx, row in tb.iterrows():
        if row["certification_status"] == "Certified disease free":
            tb_filter_country = tb[tb["country"] == row["country"]]
            tb.at[idx, "year_certified"] = int(tb_filter_country["year_certified"].fillna(0).max())
    return tb


def add_current_year(tb, tb_cases, year, changes_dict={}):
    """
    Add rows with current certification status & case numbers for each country
    tb (Table): table with certification status & case numbers until last year
    tb_cases (Table): table with case numbers for all (incl. current) years
    year (int): current year
    changes_dict (dict): changes to certification status since last year with key: country, value: certification status
    (changes_dict is empty for 2023, including it to make code reusable)
    """
    country_list = tb["country"].unique()
    last_year = year - 1

    row_dicts = []

    # add rows for current year
    for country in country_list:
        cty_dict = {"country": country, "year": year}
        # get certification status for last year
        cty_row_last_year = tb.loc[(tb["country"] == country) & (tb["year"] == last_year)]
        if country in changes_dict:
            cty_dict["certification_status"] = changes_dict[country]
            if changes_dict[country] == "Certified disease free":
                cty_dict["year_certified"] = last_year
        else:
            cty_dict["certification_status"] = cty_row_last_year["certification_status"].values[0]
            cty_dict["year_certified"] = cty_row_last_year["year_certified"].values[0]
        # get case numbers for current year
        cases_df = tb_cases.loc[(tb_cases["country"] == country) & (tb_cases["year"] == year)]
        if cases_df.empty:
            cty_dict["guinea_worm_reported_cases"] = 0
        else:
            cty_dict["guinea_worm_reported_cases"] = cases_df["guinea_worm_reported_cases"].values[0]
        row_dicts.append(cty_dict)

    # create new table with current year
    new_year_tb = Table(
        pd.DataFrame(row_dicts)[
            ["country", "year", "year_certified", "certification_status", "guinea_worm_reported_cases"]
        ]
    )

    tb = pr.concat([tb, new_year_tb], ignore_index=True)

    return tb

"""Load a meadow dataset and create a garden dataset."""

from itertools import product

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

CURRENT_YEAR = 2023


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    # fasttrack snapshot (with case numbers for 2024)
    snap_cases = Snapshot("fasttrack/2024-06-17/guinea_worm.csv")

    tb_cases = snap_cases.read().astype({"year": int})

    # garden dataset (with certification status of countries)
    ds_garden = paths.load_dataset(channel="garden", short_name="guinea_worm", version="2023-06-29")
    # Read certification table
    tb_cert = ds_garden["guinea_worm_certification"].reset_index()

    #
    # Process data.
    #
    # add missing years (with no data) to fasttrack dataset
    tb_cases = add_missing_years(tb_cases)

    # harmonize countries
    tb_cert = geo.harmonize_countries(
        df=tb_cert, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_cases = geo.harmonize_countries(
        df=tb_cases, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # remove leading spaces from "year_certified" column and cast as string
    tb_cert["year_certified"] = tb_cert["year_certified"].str.strip()

    # add year in which country was certified as disease free to all rows
    tb = pr.merge(tb_cert, tb_cases, on=["country", "year"], how="outer")

    # fill N/As with 0 (this is how we handled this previously)
    tb["guinea_worm_reported_cases"] = tb["guinea_worm_reported_cases"].fillna(0)

    # backfill certification status
    tb = backfill_certification_status(tb)

    # add rows for current year
    tb = add_current_year(tb, tb_cases, year=CURRENT_YEAR)

    # format index
    tb = tb.format(["country", "year"], short_name="guinea_worm")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    # Do not check variables metadata, as this is added in grapher step
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=False, default_metadata=ds_garden.metadata
    )

    # Save changes in the new garden dataset
    ds_garden.save()


def backfill_certification_status(tb: Table) -> Table:
    """
    Backfill certification status for countries that were endemic in the past
    """
    tb = tb.sort_values(["country", "year"])
    for cty in tb["country"].unique():
        cty_rows = tb[tb["country"] == cty].copy().dropna(subset=["certification_status"])
        if len(cty_rows) == 0:
            continue
        earliest_cert_status = cty_rows["certification_status"].iloc[0]
        year_status = cty_rows["year"].iloc[0]
        if earliest_cert_status == "Endemic":
            tb.loc[(tb["country"] == cty) & (tb["year"] < year_status), "certification_status"] = "Endemic"
            tb.loc[(tb["country"] == cty) & (tb["year"] < year_status), "year_certified"] = "Endemic"
    return tb


def add_current_year(tb: Table, tb_cases: Table, year: int = CURRENT_YEAR, changes_dict={}):
    """
    Add rows with current certification status & case numbers for each country
    tb (Table): table with certification status & case numbers until last year
    tb_cases (Table): table with case numbers for all (incl. current) years
    year (int): current year
    changes_dict (dict): changes to certification status since last year with key: country, value: certification status
    (changes_dict is empty for 2023, including it to make code reusable for future years,
    e.g. if Angola is certified in a future year, pass {"Angola": "Certified disease free"} as changes_dict for that year)
    """
    country_list = tb["country"].unique()
    last_year = year - 1

    # in case tb includes data for current year, remove it
    tb = tb.loc[tb["year"] != year]

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


def add_missing_years(tb: Table) -> Table:
    """
    Add full spectrum of year-country combinations to fast-track dataset so we have zeros where there is missing data
    """
    years = tb["year"].unique()
    countries = tb["country"].unique()
    comb_df = Table(pd.DataFrame(list(product(countries, years)), columns=["country", "year"]))

    tb = Table(pr.merge(tb, comb_df, on=["country", "year"], how="outer"), short_name=paths.short_name)

    return tb

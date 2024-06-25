"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    tb_cert = geo.harmonize_countries(
        df=tb_cert, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_cases = geo.harmonize_countries(
        df=tb_cases, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # remove leading spaces from "year_certified" column and cast as string
    tb_cert["year_certified"] = tb_cert["year_certified"].str.strip()

    # add year in which country was certified as disease free to all rows
    tb_cert = add_year_certified(tb_cert)

    # add rows for current year
    tb_cert = tb_cert[~(tb_cert["year"] == 2023)]  # data has some empty rows for 2023
    tb = add_current_year(tb_cert, tb_cases, year=2023)

    # fix data types
    tb["year_certified"] = tb["year_certified"].astype(str)

    # format index
    tb = tb.format(["country", "year"])

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


def add_year_certified(tb: Table):
    """add year in which country was certified as disease free
    by looping over all rows and setting the year_certified column
    to the maximum year_certified value for the country"""
    for idx, row in tb.iterrows():
        if row["certification_status"] == "Certified disease free":
            tb_filter_country = tb[tb["country"] == row["country"]]
            tb.at[idx, "year_certified"] = (
                pd.to_numeric(tb_filter_country["year_certified"], errors="coerce").astype("Int64").fillna(0).max()
            )
    return tb


def add_current_year(tb: Table, tb_cases: Table, year: int, changes_dict={}):
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

"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table
from owid.catalog.tables import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load ratings snapshot as a table
    snap = paths.load_snapshot("fh_ratings.xlsx")
    tb_ratings_countries = snap.read(sheet_name="Country Ratings, Statuses ", header=[1, 2])
    tb_ratings_territories = snap.read(sheet_name="Territory Ratings, Statuses", header=[1, 2])

    # Load scores snapshot as a table
    snap = paths.load_snapshot("fh_scores.xlsx")
    tb_scores = snap.read(sheet_name="FIW06-24")

    #
    # Process data.
    #
    tb_ratings = reshape_ratings(tb_c=tb_ratings_countries, tb_t=tb_ratings_territories)
    tb_scores = reshape_scores(tb_scores)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    tables = [
        tb_ratings.format(["country", "year"], short_name="fh_ratings"),
        tb_scores.format(["country", "year"], short_name="fh_scores"),
    ]

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def reshape_ratings(tb_c: Table, tb_t: Table) -> Table:
    """Format and merge country and territories ratings tables."""
    # tb_c = tb_ratings_countries.copy()
    # tb_t = tb_ratings_territories.copy()
    tb_c = _reshape_rating(tb_c)
    tb_t = _reshape_rating(tb_t)

    # Fix country values
    fixes_country = [
        ("South Africa", 1972, "civlibs_fh", "3(6)", "6"),
        ("South Africa", 1972, "polrights_fh", "2(5)", "5"),
        ("South Africa", 1972, "regime_fh", "F (NF)", "NF"),
    ]
    for fix in fixes_country:
        mask = (tb_c["country"] == fix[0]) & (tb_c["year"] == fix[1])
        if (tb_c.loc[mask, fix[2]] == fix[3]).all():
            tb_c.loc[mask, fix[2]] = fix[4]

    # Check indicator values, set int as dtype
    tb_c = _set_dtypes_indicators_ratings(tb_c)
    tb_t = _set_dtypes_indicators_ratings(tb_t)

    # Reconcile Kosovo as territory with Kosovo as country
    tb_c = tb_c.loc[~((tb_c["country"] == "Kosovo") & (tb_c["year"] < 2009))]
    tb_t = tb_t.loc[~((tb_t["country"] == "Kosovo") & (tb_t["year"] >= 2009))]

    # Add flag
    tb_c["country_fh"] = 1
    tb_t["country_fh"] = 0

    # Concatenate
    tb = concat([tb_c, tb_t], ignore_index=True)

    return tb


def reshape_scores(tb: Table) -> Table:
    """Format scores table.

    Rename columns, select relevant columns, set dtypes, fix year,
    """
    tb = cast(Table, tb.dropna(axis=1, how="all"))

    # Rename columns, keep relevant
    columns = {
        "Country/Territory": "country",
        "Edition": "year",
        "A": "electprocess_fh",
        "PR": "polrights_score_fh",
        "CL": "civlibs_score_fh",
    }
    tb = cast(Table, tb.rename(columns=columns)[columns.values()])

    # Set dtype to INT where applicable
    column_ints = ["year", "electprocess_fh", "polrights_score_fh", "civlibs_score_fh"]
    tb[column_ints] = tb[column_ints].astype("Int64")

    # Recode edition year such that it becomes observation year (instead of edition year)
    tb["year"] = tb["year"] - 1

    return tb


def _reshape_rating(tb: Table) -> Table:
    """Re-shape ratings table (country or territories).

    Output should be: country, year, pr, cl, status
    """
    tb.columns = ["country"] + [f"{col[1]}_{col[0]}" for col in tb.columns[1:]]

    # Unpivot
    tb = tb.melt(id_vars=["country"], value_name="value")

    # Correct flags
    tb["value"] = tb["value"].replace({"-": pd.NA})

    # Get year an indicator name
    tb[["indicator", "year"]] = tb["variable"].str.split("_", expand=True)
    tb = tb.drop(columns=["variable"])
    tb["indicator"] = tb["indicator"].str.strip()

    # Fix years
    year_mapping = {
        "Jan.1981-Aug. 1982": 1982,  # Consider January 1981 to August 1982 as 1982. Set tolerance in charts to 1.
        "Aug.1982-Nov.1983": 1983,  # Consider August 1982 to November 1983 as 1983.
        "Nov.1983-Nov.1984": 1984,
        "Nov.1984-Nov.1985": 1985,
        "Nov.1985-Nov.1986": 1986,
        "Nov.1986-Nov.1987": 1987,
        "Nov.1987-Nov.1988": 1988,
        "Nov.1988-Dec.1989": 1989,
    }
    tb["year"] = tb["year"].replace(year_mapping).astype(int)

    # Pivot
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()

    # Rename columns
    tb = tb.rename(
        columns={
            "CL": "civlibs_fh",
            "PR": "polrights_fh",
            "Status": "regime_fh",
        }
    )

    return tb


def _set_dtypes_indicators_ratings(tb: Table) -> Table:
    tb["civlibs_fh"] = tb["civlibs_fh"].astype("string").astype("Int64")
    tb["polrights_fh"] = tb["polrights_fh"].astype("string").astype("Int64")
    tb["regime_fh"] = (
        tb["regime_fh"]
        .astype("string")
        .replace(
            {
                "PF": "0",
                "NF": "1",
                "F": "2",
            }
        )
        .astype("Int64")
    )

    assert set(tb["civlibs_fh"].dropna()) == set(range(1, 8)), "Values for civlibs_fh are not correct!"
    assert set(tb["polrights_fh"].dropna()) == set(range(1, 8)), "Values for polrights_fh are not correct!"
    assert set(tb["regime_fh"].dropna()) == {0, 1, 2}, "Values for polrights_fh are not correct!"

    return tb

"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from owid.catalog.tables import concat

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load ratings snapshot as a table
    snap = paths.load_snapshot("fh_ratings.xlsx")
    tb_ratings_countries = snap.read(safe_types=False, sheet_name="Country Ratings, Statuses ", header=[1, 2])
    tb_ratings_territories = snap.read(safe_types=False, sheet_name="Territory Ratings, Statuses", header=[1, 2])

    # Load scores snapshot as a table
    snap = paths.load_snapshot("fh_scores.xlsx")
    tb_scores_0305 = snap.read(safe_types=False, sheet_name="FIW03-05", na_values=["-"])
    tb_scores_0624 = snap.read(safe_types=False, sheet_name="FIW06-24")
    snap_fh = paths.load_snapshot("fh.xlsx")
    tb_scores_1326 = snap_fh.read(safe_types=False, sheet_name="FIW13-26", skiprows=1)

    # Also load fh.xlsx raw for ratings supplementation (years beyond fh_ratings.xlsx coverage)
    tb_fh_raw = snap_fh.read(safe_types=False, sheet_name="FIW13-26", skiprows=1)

    #
    # Process data.
    #
    tb_ratings = reshape_ratings(tb_c=tb_ratings_countries, tb_t=tb_ratings_territories)

    # Supplement ratings with years not covered by fh_ratings.xlsx (i.e. year 2025 from edition 2026)
    ratings_max_year = int(tb_ratings["year"].max())
    tb_ratings_new = reshape_ratings_from_scores_file(tb_fh_raw, min_year=ratings_max_year + 1)
    if not tb_ratings_new.empty:
        tb_ratings = concat([tb_ratings, tb_ratings_new], ignore_index=True)

    tb_scores = reshape_scores(
        tb_0305=tb_scores_0305,
        tb_0624=tb_scores_0624,
        tb_1325=tb_scores_1326,
    )

    #
    # Save outputs.
    #
    tables = [
        tb_ratings.format(["country", "year"], short_name="fh_ratings"),
        tb_scores.format(["country", "year"], short_name="fh_scores"),
    ]

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=snap_fh.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def reshape_ratings(tb_c: Table, tb_t: Table) -> Table:
    """Format and merge country and territories ratings tables."""
    tb_c = _reshape_rating(tb_c)
    tb_t = _reshape_rating(tb_t)

    # Fix country values
    fixes_country = [
        ("South Africa", 1972, "civlibs", "3(6)", "6"),
        ("South Africa", 1972, "polrights", "2(5)", "5"),
        ("South Africa", 1972, "regime", "F (NF)", "NF"),
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
    tb_c["country_flag"] = 1
    tb_t["country_flag"] = 0

    # Concatenate
    tb = concat([tb_c, tb_t], ignore_index=True)
    tb["country_flag"] = tb["country_flag"].copy_metadata(tb_c["civlibs"])

    return tb


def reshape_scores(tb_0305, tb_0624, tb_1325):
    # Data from 2003 to 2005
    tb_0305 = reshape_scores_2003_2005(tb_0305)

    # Data from 2004 to 2024
    tb_0624 = reshape_scores_base(tb_0624)

    # Data from 2013 to 2026
    tb_1325 = reshape_scores_base(tb_1325)
    tb_1325 = tb_1325.loc[tb_1325["year"] >= 2024]

    # Combine
    assert tb_0305["year"].min() == 2002, "tb_0305 should start in 2002"
    assert tb_0624["year"].min() == tb_0305["year"].max() + 1, "tb_0624 should start the year after tb_0305 ends"
    assert tb_1325["year"].min() == tb_0624["year"].max() + 1, "tb_1325 should start when tb_0624 ends"
    tb = pr.concat([tb_0305, tb_0624, tb_1325], ignore_index=True)

    # Set dtypes
    tb = tb.astype({"country": "string"})
    return tb


def reshape_ratings_from_scores_file(tb: Table, min_year: int) -> Table:
    """Extract ratings (polrights, civlibs, regime, country_flag) from the all-data scores file.

    This is used to supplement fh_ratings.xlsx for years not yet covered by the historical
    ratings file (e.g. year 2025 from FIW edition 2026).
    """
    # Rename columns to standard names
    tb = tb.rename(
        columns={
            "Country/Territory": "country",
            "Edition": "year",
            "PR rating": "polrights",
            "CL rating": "civlibs",
            "Status": "regime",
            "C/T": "country_flag",
        }
    )
    # Keep only relevant columns
    tb = tb[["country", "year", "polrights", "civlibs", "regime", "country_flag"]].copy()

    # Recode edition year → observation year
    tb["year"] = tb["year"] - 1

    # Filter to new years only
    tb = tb.loc[tb["year"] >= min_year].copy()

    if tb.empty:
        return tb

    # Recode country_flag: c → 1, t → 0
    tb["country_flag"] = tb["country_flag"].replace({"c": 1, "t": 0})

    # Apply same dtype conversions as _set_dtypes_indicators_ratings
    tb["civlibs"] = tb["civlibs"].astype("string").astype("Int64")
    tb["polrights"] = tb["polrights"].astype("string").astype("Int64")
    tb["regime"] = tb["regime"].astype("string").replace({"NF": "0", "PF": "1", "F": "2"}).astype("Int64")
    tb["country_flag"] = tb["country_flag"].astype("Int64")
    tb["country"] = tb["country"].astype("string")
    tb["year"] = tb["year"].astype(int)

    assert set(tb["civlibs"].dropna()) == set(range(1, 8)), "Values for civlibs (from scores file) are not correct!"
    assert set(tb["polrights"].dropna()) == set(range(1, 8)), "Values for polrights (from scores file) are not correct!"
    assert set(tb["regime"].dropna()) == {0, 1, 2}, "Values for regime (from scores file) are not correct!"

    return tb


def reshape_scores_2003_2005(tb: Table) -> Table:
    id_vars = ["Country/Territory", "C/T?"]  # Columns to keep as identifiers
    value_vars = [col for col in tb.columns if col.startswith("FIW")]  # Columns to melt
    tb = pr.melt(tb, id_vars=id_vars, value_vars=value_vars, var_name="metric", value_name="value")
    tb[["year", "metric_type"]] = tb["metric"].str.extract(r"FIW(\d{2})\s(.+)")

    # Convert year code to full year (03 -> 2003, 04 -> 2004, 05 -> 2005)
    tb["year"] = 2000 + tb["year"].astype(int)

    tb = tb.pivot(index=id_vars + ["year"], columns="metric_type", values="value").reset_index()
    # Flatten column names
    tb.columns.name = None

    tb = reshape_scores_base(tb)
    return tb


def reshape_scores_base(tb: Table) -> Table:
    """Format scores table.

    Rename columns, select relevant columns, set dtypes, fix year,
    """
    tb = tb.dropna(axis=1, how="all")

    # Rename columns
    columns_rename = {
        "Country/Territory": "country",
        "Edition": "year",
        "A": "electprocess",
        "PR": "polrights_score",
        "CL": "civlibs_score",
        "Total": "total_score",
    }
    tb = tb.rename(columns=columns_rename)
    # Keep relevant columns
    columns = [col for col in columns_rename.values() if col in tb.columns]
    tb = tb.loc[:, columns]

    # Set dtype to INT where applicable
    # column_ints = ["year", "electprocess", "polrights_score", "civlibs_score", "total_score"]
    # tb[column_ints] = tb[column_ints].astype("Int64")

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
            "CL": "civlibs",
            "PR": "polrights",
            "Status": "regime",
        }
    )

    return tb


def _set_dtypes_indicators_ratings(tb: Table) -> Table:
    tb["civlibs"] = tb["civlibs"].astype("string").astype("Int64")
    tb["polrights"] = tb["polrights"].astype("string").astype("Int64")
    tb["regime"] = (
        tb["regime"]
        .astype("string")
        .replace(
            {
                "NF": "0",
                "PF": "1",
                "F": "2",
            }
        )
        .astype("Int64")
    )

    assert set(tb["civlibs"].dropna()) == set(range(1, 8)), "Values for civlibs are not correct!"
    assert set(tb["polrights"].dropna()) == set(range(1, 8)), "Values for polrights are not correct!"
    assert set(tb["regime"].dropna()) == {0, 1, 2}, "Values for polrights are not correct!"

    return tb

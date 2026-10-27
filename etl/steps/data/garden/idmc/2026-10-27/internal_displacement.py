"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# shorten column names (keep originals as they come from IDMC like this):
D_T_D_R = "disaster_total_displacement_rounded"
D_T_D = "disaster_total_displacement"
D_N_D_R = "disaster_new_displacement_rounded"
D_N_D = "disaster_new_displacement"
C_T_D_R = "conflict_total_displacement_rounded"
C_T_D = "conflict_total_displacement"
C_N_D_R = "conflict_new_displacement_rounded"
C_N_D = "conflict_new_displacement"


def return_if_na(col, na_col, tb):
    """return the table filtered for rows where na_col is na and col is not na"""
    return tb[tb[na_col].isna() & tb[col].notna()]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("internal_displacement")

    # Read table from meadow dataset.
    tb = ds_meadow.read("internal_displacement")
    sanity_check_inputs(tb)

    tb = tb.rename(columns={"country_name": "country"}, errors="raise")

    tb = tb.drop(columns=["iso3"], errors="raise")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # fill rounded columns with non-rounded values where rounded values are na
    tb[C_T_D_R] = tb[C_T_D_R].fillna(tb[C_T_D])
    tb[C_N_D_R] = tb[C_N_D_R].fillna(tb[C_N_D])
    tb[D_T_D_R] = tb[D_T_D_R].fillna(tb[D_T_D])
    tb[D_N_D_R] = tb[D_N_D_R].fillna(tb[D_N_D])

    # fill n/a values with 0, as no displacements were recorded
    tb = na_to_zero(tb)

    # add total displacements (conflict + disaster)
    tb["total_displacement"] = tb[C_T_D] + tb[D_T_D]
    # NOTE: IDMC reports disaster IDPs before 2019 only for some countries (blanks there mean no data, not zero),
    # so we only calculate total IDPs from 2019. Re-check with IDMC on each update.
    tb.loc[tb["year"] < 2019, "total_displacement"] = None
    tb["total_displacement_rounded"] = tb["total_displacement"].apply(round_idmc_style)
    tb["total_new_displacement"] = tb[C_N_D] + tb[D_N_D]
    tb["total_new_displacement_rounded"] = tb["total_new_displacement"].apply(round_idmc_style)

    # fill n/a values with 0 in the new total columns
    # stock IDPs starting in 2019 (full disaster IDP coverage starts in 2019)
    tb.loc[tb["year"] >= 2019, "total_displacement_rounded"] = tb.loc[
        tb["year"] >= 2019, "total_displacement_rounded"
    ].fillna(0)
    # new displacements starting in 2008 (conflict and disaster displacements both start in 2008)
    tb.loc[tb["year"] >= 2008, "total_new_displacement_rounded"] = tb.loc[
        tb["year"] >= 2008, "total_new_displacement_rounded"
    ].fillna(0)

    # Add population after na_to_zero, so the rows it creates also get per-1,000 values.
    tb = paths.regions.add_population(tb)

    columns_to_calculate = [col for col in tb.columns if col not in ["country", "year", "population"]]

    tb = calculate_shares(tb, columns_to_calculate)
    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    expected_columns = {"country_name", "year", "iso3", C_T_D_R, C_T_D, C_N_D_R, C_N_D, D_T_D_R, D_T_D, D_N_D_R, D_N_D}
    assert set(tb.columns) == expected_columns, f"Unexpected columns: {set(tb.columns) ^ expected_columns}"
    assert not tb.duplicated(subset=["country_name", "year"]).any(), "Duplicate (country, year) rows in IDMC data."
    assert tb["year"].min() >= 2008, (
        f"Data before 2008 found ({tb['year'].min()}); the zero-filling assumes 2008 is the first year."
    )
    value_columns = sorted(expected_columns - {"country_name", "year", "iso3"})
    assert (tb[value_columns].min() >= 0).all(), "Negative displacement figures found."


def sanity_check_outputs(tb: Table) -> None:
    # Conflict figures and disaster displacements are complete from 2008, disaster IDPs from 2019.
    for col in [C_T_D, C_T_D_R, C_N_D, C_N_D_R, D_N_D, D_N_D_R, "total_new_displacement_rounded"]:
        assert tb[col].notna().all(), f"Missing values in {col}."
    recent = tb["year"] >= 2019
    for col in [D_T_D, D_T_D_R, "total_displacement", "total_displacement_rounded"]:
        assert tb.loc[recent, col].notna().all(), f"Missing values in {col} from 2019."
    # Before 2019, disaster IDPs are only reported for some countries, so they must not be zero-filled, and no total is calculated.
    assert not (tb.loc[~recent, D_T_D] == 0).any(), "Disaster IDPs before 2019 were filled with zeros."
    assert tb.loc[~recent, ["total_displacement", "total_displacement_rounded"]].isna().all().all(), (
        "Total IDPs calculated before 2019."
    )

    # Totals are the sum of their parts (cast to float, as the source columns are unsigned integers).
    total = tb.loc[recent, C_T_D].astype(float) + tb.loc[recent, D_T_D].astype(float)
    assert (tb.loc[recent, "total_displacement"].astype(float) == total).all(), (
        "Total IDPs don't equal conflict + disaster IDPs."
    )
    total_new = tb[C_N_D].astype(float) + tb[D_N_D].astype(float)
    assert (tb["total_new_displacement"].astype(float) == total_new).all(), (
        "Total new displacements don't equal conflict + disaster."
    )

    # Rounded totals stay within IDMC's rounding (2 significant digits, i.e. at most 5% off).
    for col in ["total_displacement", "total_new_displacement"]:
        raw, rounded = tb[col].astype(float), tb[f"{col}_rounded"].astype(float)
        rel_diff = ((rounded - raw).abs() / raw.clip(lower=1)).max()
        assert rel_diff <= 0.05, f"Rounded {col} differs from the raw value by {rel_diff:.1%}."

    value_columns = [col for col in tb.columns if col not in ["country", "year"]]
    assert (tb[value_columns].min() >= 0).all(), "Negative values in output."


def calculate_shares(tb, columns_to_calculate):
    for displacement_col in columns_to_calculate:
        share_col_name = f"{displacement_col}_per_thousand"
        tb[share_col_name] = (tb[displacement_col] / tb["population"]) * 1000

    tb = tb.drop(columns=["population"], errors="raise")
    return tb


def round_idmc_style(x):
    """Round numbers according to IDMC style.

    - Numbers <= 100,000 are rounded to 2 significant digits.
    - Numbers > 100,000 are rounded to the nearest 1,000.
    """
    if x <= 100000:
        return round(x, -len(str(int(x))) + 2)
    else:
        return round(x, -3)


def na_to_zero(tb):
    """Fill all NaN values with 0. If a country year combination has no row (and no NaN value), create one with 0 values.
    Start of data collection:
    - 2008: natural disaster displacements, conflict displacements + conflict IDPs
    - 2019: disaster IDPs (before 2019, IDMC reports them only for some countries, so blanks stay as no data)
    """
    if tb["year"].min() < 2008:
        raise ValueError(f"Year {tb['year'].min()} is before 2008, which is not expected in the dataset.")

    # first, add a row for every country-year combination that is missing (reindexing keeps the columns' metadata)
    all_combinations = pd.MultiIndex.from_product(
        [tb["country"].unique(), tb["year"].unique()], names=["country", "year"]
    )
    tb = tb.set_index(["country", "year"]).reindex(all_combinations).reset_index()

    # then, fill NaN values with 0 for years where data is expected to exist
    # 2008 and onwards has natural disaster displacements
    tb.loc[tb["year"] >= 2008, D_N_D_R] = tb.loc[tb["year"] >= 2008, D_N_D_R].fillna(0)
    tb.loc[tb["year"] >= 2008, D_N_D] = tb.loc[tb["year"] >= 2008, D_N_D].fillna(0)

    # 2008 and onwards has conflict displacements and conflict IDPs
    tb.loc[tb["year"] >= 2008, C_T_D_R] = tb.loc[tb["year"] >= 2008, C_T_D_R].fillna(0)
    tb.loc[tb["year"] >= 2008, C_T_D] = tb.loc[tb["year"] >= 2008, C_T_D].fillna(0)
    tb.loc[tb["year"] >= 2008, C_N_D_R] = tb.loc[tb["year"] >= 2008, C_N_D_R].fillna(0)
    tb.loc[tb["year"] >= 2008, C_N_D] = tb.loc[tb["year"] >= 2008, C_N_D].fillna(0)

    # 2019 and onwards has disaster IDPs
    tb.loc[tb["year"] >= 2019, D_T_D_R] = tb.loc[tb["year"] >= 2019, D_T_D_R].fillna(0)
    tb.loc[tb["year"] >= 2019, D_T_D] = tb.loc[tb["year"] >= 2019, D_T_D].fillna(0)

    return tb

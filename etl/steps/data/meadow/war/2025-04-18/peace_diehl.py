"""Load a snapshot and create a meadow dataset."""

import numpy as np
import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot
    snap = paths.load_snapshot("peace_diehl.zip")

    with snap.open_archive():
        tb = snap.read_from_archive("peacedata3.1.1/peacedata3.1.1.csv", header=None, na_values=[".", None])

    #
    # Process data.
    #
    # Check first column is Int
    assert tb[0].dtype == pd.Int64Dtype()
    # Sanity checks: Columns are alternating from "XXXX-YYYY" (ods), to `float value` (evens)
    for i in range(1, tb.shape[1]):
        if i % 2 == 0:
            assert tb[i].dtype == pd.Float64Dtype()
        else:
            assert tb[i].dropna().str.contains("-").all()

    # Reshape dataframe
    # Concatenate related columns; go from (code, years_1, peace_scale_level_1, years_2, peace_scale_level_2, ...) -> (code, years, peace_scale_level)
    tbs = []
    for i in range(1, tb.shape[1], 2):
        tb_ = tb.loc[:, [0, i, i + 1]]
        tb_.columns = ["code", "years", "peace_scale_level"]
        tbs.append(tb_)
    tb = pr.concat(tbs, ignore_index=True, short_name=paths.short_name)

    # Drop all-NaN rows
    tb = tb.dropna(subset=["years", "peace_scale_level"], how="all")

    # Sanity check
    assert tb.isna().sum().sum() == 0, "Unexpected NaNs!"

    # Replace -9 to NaN
    tb["peace_scale_level"] = tb["peace_scale_level"].replace({-9: np.nan})

    # Column code as "string"
    tb["code"] = tb["code"].astype("string")

    # Map code -> code_1, code_2
    ## Check on lengths. Can be 4, 5 or 6
    assert set(tb["code"].str.len()) == {4, 5, 6}, "Unexpected lengths!"
    ## Standardise dyad codes (6-length codes)
    mask_4 = tb["code"].str.len() == 4
    tb.loc[mask_4, "code"] = "00" + tb.loc[mask_4, "code"]
    mask_5 = tb["code"].str.len() == 5
    tb.loc[mask_5, "code"] = "0" + tb.loc[mask_5, "code"]
    # Final check and split
    assert (tb["code"].str.len() == 6).all(), "Unexpected format of `code`! All rows shoul have 6 characters!"
    tb["code_1"] = tb["code"].str[:3]
    tb["code_2"] = tb["code"].str[3:]

    # Split years: Map years -> time_start, time_end
    tb[["time_start", "time_end"]] = (
        tb["years"].str.split("-", expand=True).rename(columns={0: "time_start", 1: "time_end"})
    )

    # Keep relevant columns
    tb = tb.loc[:, ["code_1", "code_2", "time_start", "time_end", "peace_scale_level"]]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["code_1", "code_2", "time_start", "time_end"])

    #
    # Save outputs
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

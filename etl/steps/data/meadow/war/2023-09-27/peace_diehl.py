"""Load a snapshot and create a meadow dataset."""

import zipfile

import numpy as np
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("peace_diehl.zip")

    # Load data from snapshot.
    with zipfile.ZipFile(snap.path) as z:
        # open the csv file in the dataset
        with z.open("peacedata3.1/peacedata3.1.csv") as f:
            # read the dataset
            tb = pr.read_fwf(f, header=None, names=["data"], metadata=snap.to_table_metadata(), origin=snap.m.origin)

    meta_origins = tb["data"].metadata.origins

    # Split into multiple columns (returned columns have numeric names, i.e. metadata is lost)
    tb = tb["data"].str.split(",", expand=True)

    # Rename columns
    tb = tb.rename(columns={col: str(col) for col in tb.columns})

    # Ensure metadata in indicators is preserved
    for column in tb.all_columns:
        tb[column].metadata.origins = meta_origins

    #
    # Process data.
    #
    # Reshape dataframe
    # Concatenate related columns; go from (code, years_1, peace_scale_level_1, years_2, peace_scale_level_2, ...) -> (code, years, peace_scale_level)
    tbs = []
    for i in range(1, tb.shape[1], 2):
        tb_ = tb[["0", str(i), str(i + 1)]].rename(
            columns={"0": "code", str(i): "years", str(i + 1): "peace_scale_level"}
        )
        tbs.append(tb_)
    tb = pr.concat(tbs, ignore_index=True, short_name=paths.short_name)

    # Replace: None -> NaN
    tb = tb.replace({None: np.nan, "": np.nan, ".": np.nan})
    # Drop all-NaN rows
    tb = tb.dropna(subset=["years", "peace_scale_level"], how="all")

    # Uniform peace_scale_level values (e.g. "0.50", "0.5" -> "0.5")
    tb["peace_scale_level"] = tb["peace_scale_level"].astype(float)

    # Sanity check
    assert tb.isna().sum().sum() == 0, "Unexpected NaNs!"

    # Replace -9 to NaN
    tb["peace_scale_level"] = tb["peace_scale_level"].replace({-9: np.nan})

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

    # Keep relevant
    tb = tb[["code_1", "code_2", "time_start", "time_end", "peace_scale_level"]]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["code_1", "code_2", "time_start", "time_end"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

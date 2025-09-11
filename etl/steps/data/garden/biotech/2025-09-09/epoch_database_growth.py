import datetime as dt

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def get_maximum_per_year(year: int, df):
    """Get the maximum value for a given year."""
    start_year = dt.datetime(year, 1, 1)
    end_year = dt.datetime(year, 12, 31)
    df_year = df[(df["Date"] >= start_year) & (df["Date"] <= end_year)]
    if not df_year.empty:
        return df_year["Value"].max()
    else:
        return pd.NA


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    ds_meadow = paths.load_dataset("epoch_database_growth")

    # Read table.
    tb_uni_prot = ds_meadow["uniprot"]
    tb_mgnify = ds_meadow["mgnify"]
    tb_pdb = ds_meadow["pdb"]
    tb_alpha_fold = ds_meadow["alpha_fold"]
    tb_esm_atlas = ds_meadow["esm_atlas"]
    tb_ena = ds_meadow["ena"]
    tb_gb_all = ds_meadow["gb_all"]
    tb_gb_traditional = ds_meadow["gb_traditional"]
    tb_ddbj = ds_meadow["ddbj"]
    tb_refseq = ds_meadow["refseq"]

    tb_uni_prot["date"] = pd.to_datetime(tb_uni_prot["date"])

    tb_mgnify["date"] = pd.to_datetime(tb_mgnify["date"])

    tb_alpha_fold["date"] = pd.to_datetime(tb_alpha_fold["release_time"])

    tb_esm_atlas["date"] = pd.to_datetime(tb_esm_atlas["release_date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

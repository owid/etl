import zipfile

import pandas as pd
from owid.catalog import Table
from owid.catalog.utils import underscore
from pandas.api.types import is_numeric_dtype  # type: ignore
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    log.info("wdi.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot()

    # Load data from snapshot.
    zf = zipfile.ZipFile(snap.path)

    df = pd.read_csv(zf.open("WDICSV.csv"))

    df.dropna(how="all", axis=1, inplace=True)

    # drops rows with only NaN values
    years = df.columns[df.columns.str.contains(r"^\d{4}$")].sort_values().tolist()
    df.dropna(subset=years, how="all", inplace=True)

    # converts columns and indicator_code to snake case
    df.columns = df.columns.map(lambda x: x if x in years else underscore(x))
    orig_indicator_code = df["indicator_code"].copy()
    df["indicator_code"] = df["indicator_code"].astype("category").map(underscore)
    indicator_code_map = dict(zip(df["indicator_code"], orig_indicator_code))

    assert df["country_name"].notnull().all()
    assert df["indicator_code"].notnull().all()
    assert df[years].apply(lambda s: is_numeric_dtype(s), axis=0).all(), "One or more {year} columns is non-numeric"

    # variable code <-> variable name should be a 1:1 mapping
    assert (
        df.groupby("indicator_code", observed=True)["indicator_name"].apply(lambda gp: gp.nunique()) == 1
    ).all(), "A variable code in `WDIData.csv` has multiple variable names."
    assert (
        df.groupby("indicator_name", observed=True)["indicator_code"].apply(lambda gp: gp.nunique()) == 1
    ).all(), "A variable name in `WDIData.csv` has multiple variable codes."

    # reshapes data from `country indicator 1960 1961 ...` format to long format `country indicator_code year value`
    df_long = (
        df.set_index(["country_name", "indicator_code"])[years]
        .stack()
        .sort_index()
        .reset_index()
        .rename(columns={"country_name": "country", "level_2": "year", 0: "value"})
    )

    # reshape from long format to wide `country year EG.CFT.ACCS.ZS SH.HIV.INCD.YG ...`
    df_wide = (
        df_long.set_index(["country", "year", "indicator_code"], verify_integrity=True)
        .squeeze()
        .unstack("indicator_code")
        .dropna(how="all")
    )
    assert not df_wide.isnull().all(axis=1).any(), "Unexpected state: One or more rows contains only NaN values."

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df_wide, short_name=paths.short_name, underscore=True)

    # Add origin to all indicators
    for col in tb.columns:
        tb[col].m.origins = [snap.m.origin]

        # Add original code as titles
        tb[col].m.title = indicator_code_map[col]

    # Load metadata from snapshot.
    tb_meta = Table(pd.read_csv(zf.open("WDISeries.csv")), short_name="wdi_metadata", underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb, tb_meta], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wdi.end")

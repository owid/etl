import zipfile
from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, DatasetMeta, Table, TableMeta
from owid.catalog.utils import underscore
from owid.walden import Catalog
from pandas.api.types import is_numeric_dtype  # type: ignore


def run(dest_dir: str) -> None:
    # retrieves raw data from walden
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem
    namespace = Path(__file__).parent.parent.stem
    walden_ds = Catalog().find_one(
        namespace=namespace, short_name=fname, version=version
    )
    local_file = walden_ds.ensure_downloaded()
    zf = zipfile.ZipFile(local_file)
    df = pd.read_csv(zf.open("WDIData.csv"))

    df.dropna(how="all", axis=1, inplace=True)

    # drops rows with only NaN values
    years = df.columns[df.columns.str.contains(r"^\d{4}$")].sort_values().tolist()
    df.dropna(subset=years, how="all", inplace=True)

    # converts columns and indicator_code to snake case
    df.columns = df.columns.map(lambda x: x if x in years else underscore(x))
    df["indicator_code"] = df["indicator_code"].apply(underscore)

    assert df["country_name"].notnull().all()
    assert df["indicator_code"].notnull().all()
    assert (
        df[years].apply(lambda s: is_numeric_dtype(s), axis=0).all()
    ), "One or more {year} columns is non-numeric"

    # variable code <-> variable name should be a 1:1 mapping
    assert (
        df.groupby("indicator_code")["indicator_name"].apply(lambda gp: gp.nunique())
        == 1
    ).all(), "A variable code in `WDIData.csv` has multiple variable names."
    assert (
        df.groupby("indicator_name")["indicator_code"].apply(lambda gp: gp.nunique())
        == 1
    ).all(), "A variable name in `WDIData.csv` has multiple variable codes."

    # reshapes data from `country indicator 1960 1961 ...` format
    # to `country year EG.CFT.ACCS.ZS SH.HIV.INCD.YG ...` format
    df_reshaped = (
        df.set_index(["country_name", "indicator_code"])[years]
        .stack()
        .sort_index()
        .reset_index()
        .rename(columns={"country_name": "country", "level_2": "year", 0: "value"})
        .set_index(["country", "year", "indicator_code"], verify_integrity=True)
        .squeeze()
        .unstack("indicator_code")
        .dropna(how="all")
    )
    assert (
        not df_reshaped.isnull().all(axis=1).any()
    ), "Unexpected state: One or more rows contains only NaN values."

    # creates the dataset and adds a table
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = DatasetMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        namespace=walden_ds.namespace,
        description=walden_ds.description,
    )
    tb = Table(df_reshaped)
    tb.metadata = TableMeta(
        short_name=Path(__file__).stem,
        title=walden_ds.name,
        description=walden_ds.description,
        primary_key=list(df_reshaped.index.names),
    )
    ds.add(tb)
    ds.save()

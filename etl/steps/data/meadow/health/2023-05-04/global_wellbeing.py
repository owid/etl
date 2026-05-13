"""Load a snapshot and create a meadow dataset.

This snapshot is an xlsx with a 3-level MultiIndex header and merged cells across
sheets, which doesn't fit the standard `snap.read_csv → tb.format` flow. We read with
pandas to do the reshape, then wrap in a Table and attach the snapshot's origin to
every column so it propagates through garden → grapher.
"""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("global_wellbeing.xlsx")

    dfs = pd.read_excel(snap.path, sheet_name=None, header=[0, 1, 2])
    _sanity_checks(dfs)

    df = _combine_sheets(dfs["Country Level"], dfs["Global"])
    # Drop the innermost ("% Response Rate") header level — uninformative.
    df = df.droplevel(level=2, axis=1)

    col_id = list(df.columns[:3])
    df = df.melt(id_vars=col_id, value_name="share")
    df = df.rename(columns={"variable_0": "question", "variable_1": "answer"})

    df["country"] = df[df.columns[0]].ffill()
    df["dimension"] = df[df.columns[1]].ffill()
    df["dimension"] = "(" + df["dimension"] + "?) " + df[df.columns[2]]

    df = df.set_index(["country", "dimension", "question", "answer"], verify_integrity=True)[["share"]]

    tb = Table(df, short_name=paths.short_name, underscore=True)

    # Propagate snapshot origin onto every column (no metadata survives the
    # pandas reshape above, so we re-attach it here).
    if snap.metadata.origin is not None:
        for col in tb.columns:
            tb[col].metadata.origins = [snap.metadata.origin]

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()


def _sanity_checks(dfs: dict[str, pd.DataFrame]) -> None:
    expected = {"Country Level", "Global"}
    missing = expected.difference(dfs.keys())
    assert not missing, f"There are some missing sheets! {missing}"


def _combine_sheets(df_countries: pd.DataFrame, df_world: pd.DataFrame) -> pd.DataFrame:
    df_world.columns = df_countries.columns[1:]
    df_world[df_countries.columns[0]] = "World"
    return pd.concat([df_countries, df_world], ignore_index=True)

"""Load the three CSO sources of long-run Irish net migration and create a meadow dataset.

- Census 1926 General Report (hand-entered): average yearly net emigration by sex for each
  intercensal period 1871-1926, plus census populations, for the 26-county area.
- Census 2022, table F1005: net migration and population for each intercensal period 1926-2022,
  estimated as the census residual.
- Population and Migration Estimates, table PEA15: annual net migration and population from 1951
  (years ending in April), in thousands.
"""

import json

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def read_jsonstat(snap: Snapshot) -> tuple[dict, pd.DataFrame]:
    """Read a PxStat JSON-stat 2.0 file into a long dataframe with one column per dimension."""
    with open(snap.path) as f:
        data = json.load(f)
    dims = data["dimension"]
    order = data["id"]
    labels = {k: list(dims[k]["category"]["label"].values()) for k in order}
    shape = [len(labels[k]) for k in order]
    values = np.array(data["value"], dtype=object).reshape(shape)
    index = pd.MultiIndex.from_product([labels[k] for k in order], names=order)
    df = pd.DataFrame({"value": values.ravel()}, index=index).reset_index()
    return data, df


def parse_pea15(snap: Snapshot) -> Table:
    """Annual net migration and population, 1951-2025, in thousands."""
    _, df = read_jsonstat(snap)
    df = df.rename(columns={"TLIST(A1)": "year", "C02541V03076": "component"})
    df = df[df["component"].isin(["Net migration", "Population"])]
    df = df.pivot(index="year", columns="component", values="value").reset_index()
    df.columns.name = None
    df = df.rename(columns={"Net migration": "net_migration", "Population": "population"})
    df["year"] = df["year"].astype(int)
    for col in ["net_migration", "population"]:
        df[col] = pd.to_numeric(df[col], errors="raise")

    tb = snap.read_from_df(df)
    assert tb["year"].min() == 1951, "PEA15 no longer starts in 1951."
    assert tb["year"].max() >= 2025, "PEA15 should reach at least 2025."
    assert tb["net_migration"].notna().all() and tb["population"].notna().all(), "Missing values in PEA15."
    # Spot-check (thousands): net migration in 1958, the deepest year of the 1950s emigration wave.
    assert tb.loc[tb["year"] == 1958, "net_migration"].item() == -58.0
    return tb


def parse_f1005(snap: Snapshot) -> Table:
    """Net migration (period total, persons) and end-of-period population per intercensal period."""
    _, df = read_jsonstat(snap)
    df = df.rename(columns={"C02691V03258": "period", "C03853V04603": "region"})
    df = df[(df["region"] == "State") & (df["STATISTIC"].isin(["Net migration", "Population"]))]
    df = df.pivot(index="period", columns="STATISTIC", values="value").reset_index()
    df.columns.name = None
    df = df.rename(columns={"Net migration": "net_migration", "Population": "population_end"})
    df["period_start"] = df["period"].str.split(" - ").str[0].astype(int)
    df["period_end"] = df["period"].str.split(" - ").str[1].astype(int)
    df = df.sort_values("period_start")[["period_start", "period_end", "net_migration", "population_end"]]
    for col in ["net_migration", "population_end"]:
        df[col] = pd.to_numeric(df[col], errors="raise")

    tb = snap.read_from_df(df)
    assert tb["period_start"].min() == 1926 and tb["period_end"].max() >= 2022, "Expected coverage 1926-2022."
    # Periods must chain without gaps: each period starts where the previous one ended.
    assert (tb["period_start"].iloc[1:].values == tb["period_end"].iloc[:-1].values).all(), "Gap between periods."
    # Spot-checks: the 1926-1936 residual, and the 2022 census population as the last period's end.
    assert tb.loc[tb["period_start"] == 1926, "net_migration"].item() == -166_751
    assert tb.loc[tb["period_end"] == 2022, "population_end"].item() == 5_149_139
    return tb


def parse_census_1926(snap: Snapshot) -> Table:
    """Hand-entered 1871-1926 intercensal net emigration and census populations."""
    tb = snap.read_csv()
    assert tb["period_start"].min() == 1871 and tb["period_end"].max() == 1926, "Expected coverage 1871-1926."
    assert (tb["period_start"].iloc[1:].values == tb["period_end"].iloc[:-1].values).all(), "Gap between periods."
    assert (tb[["net_emigration_males", "net_emigration_females"]] > 0).all().all()
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    snap_pea15 = paths.load_snapshot("ireland_population_change.json")
    snap_f1005 = paths.load_snapshot("ireland_components_of_population_change.json")
    snap_1926 = paths.load_snapshot("ireland_net_emigration_1871_1926.csv")

    #
    # Process data.
    #
    tb_annual = parse_pea15(snap_pea15)
    tb_intercensal = parse_f1005(snap_f1005)
    tb_1926 = parse_census_1926(snap_1926)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(
        tables=[
            tb_annual.format(["year"], short_name="annual_estimates"),
            tb_intercensal.format(["period_start"], short_name="intercensal_1926_2022"),
            tb_1926.format(["period_start"], short_name="intercensal_1871_1926"),
        ],
        default_metadata=snap_pea15.metadata,
    )
    ds_meadow.save()

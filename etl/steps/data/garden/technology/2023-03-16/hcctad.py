"""Load a meadow dataset and create a garden dataset."""

import os

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_meadow = paths.load_dataset("hcctad")
    tb = ds_meadow.read("hcctad")

    vars_df = pd.read_csv(os.path.join(paths.directory, "hcctad_variables.csv"))

    tb = _filter_countries(tb)
    tb = _convert_and_rename(tb, vars_df)
    tb = _aggregate_variables(tb)
    tb = _fix_us_freight(tb)
    tb = _reshape_to_wide(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def _filter_countries(tb: Table) -> Table:
    """Keep only the US and UK series — most complete and most relevant for OWID readers."""
    tb["country"] = tb["country"].astype(str)
    return tb[tb["country"].isin(["United States", "United Kingdom"])]


def _convert_and_rename(tb: Table, vars_df: pd.DataFrame) -> Table:
    """Apply per-variable unit conversion and rename to canonical names."""
    vars_tb = Table(vars_df)
    tb = pr.merge(tb, vars_tb, on="variable", validate="many_to_one")
    tb["value"] = tb["value"] * tb["multiply_by"]
    tb = tb.drop(columns="multiply_by")
    tb = tb.drop(columns="variable").rename(columns={"new_name": "variable"})
    return tb


def _fix_us_freight(tb: Table) -> Table:
    """Remove the 1959 US railway-freight typo (~10x the surrounding years).

    See https://github.com/owid/owid-issues/issues/993#issuecomment-1473332534
    """
    bad = (
        (tb["variable"] == "Railway freight traffic (metric ton-km)")
        & (tb["country"] == "United States")
        & (tb["year"] == 1959)
    )
    return tb[~bad]


def _aggregate_variables(tb: Table) -> Table:
    """Sum across (country, variable, year) to combine sub-types of steel / textile spindles."""
    return tb.groupby(["country", "variable", "year"], as_index=False).sum()


def _reshape_to_wide(tb: Table) -> Table:
    """Pivot so countries become columns and technologies become rows (entities)."""
    tb = tb.pivot(
        index=["variable", "year"],
        columns="country",
        values="value",
        join_column_levels_with="_",
    )
    tb = tb.rename(columns={"variable": "country"})
    return tb

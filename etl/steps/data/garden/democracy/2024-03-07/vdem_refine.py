from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Table, Variable


def run(tb: Table) -> Table:
    tb = add_years_in_democracy(tb)
    tb = add_categories_years_in_democracy(tb)
    tb = add_categories_women_in_parliament(tb)
    # Ensure int types
    tb_ = tb.astype(
        {
            "regime_row_owid": "Int64",
            "regime_amb_row_owid": "Int64",
            "wom_hoe_vdem": "Int64",
        }
    )

    tb_ = cast(Table, tb_)
    return tb_


def add_years_in_democracy(tb: Table) -> Table:
    """Add years in democracy.

    Two types of counters are generated:
        - Number of years consecutively: Counts number of years consecutively in democracy.
        - Number of years: Counts all the years that the country has been a democracy.

    We consider two types of democracies:

        - Electoral democracy
        - Liberal democracy
    """

    def _add_regime_type(tb: Table, column_new: str, threshold: int) -> Table:
        tb[column_new] = tb["regime_row_owid"]
        tb.loc[tb["regime_row_owid"] <= threshold, column_new] = 0
        tb.loc[tb["regime_row_owid"] > threshold, column_new] = 1
        return tb

    # Add flags
    tb = _add_regime_type(tb, "regime_electdem", 1)
    tb = _add_regime_type(tb, "regime_libdem", 2)

    # Add age of democracy
    tb["num_years_in_electdem_consecutive"] = tb.groupby(["country", tb["regime_electdem"].fillna(0).eq(0).cumsum()])[
        "regime_electdem"
    ].cumsum()
    tb["num_years_in_libdem_consecutive"] = tb.groupby(["country", tb["regime_libdem"].fillna(0).eq(0).cumsum()])[
        "regime_libdem"
    ].cumsum()
    # Add experience with democracy
    tb["num_years_in_electdem"] = tb.groupby("country")["regime_electdem"].cumsum()
    tb["num_years_in_libdem"] = tb.groupby("country")["regime_libdem"].cumsum()

    # Drop unused columns
    tb = tb.drop(columns=["regime_electdem", "regime_libdem"])
    return tb


def add_categories_years_in_democracy(tb: Table) -> Table:
    """Add categories for "years in democracy" indicators.

    We group countries in categories based on how long they've been - consecutively - a democracy (liberal and electoral).
    """
    bins = [
        0,
        18,
        30,
        60,
        90,
        np.inf,
    ]
    labels = [
        "1-18 years",
        "19-30 years",
        "31-60 years",
        "61-90 years",
        "91+ years",
    ]
    # 1. Create variable for age group of electoral demcoracies:
    column = "num_years_in_electdem"
    column_cat = f"{column}_cat"
    tb[column_cat] = pd.cut(tb[column], bins=bins, labels=labels)
    tb[column_cat] = Variable(tb[column_cat]).copy_metadata(tb.loc[:, column])

    ## Add additional categories
    tb[column_cat] = tb[column_cat].cat.add_categories(["electoral autocracy", "closed autocracy"])
    tb.loc[(tb["regime_row_owid"] == 1) & tb[column_cat].isna(), column_cat] = "electoral autocracy"
    tb.loc[(tb["regime_row_owid"] == 0) & tb[column_cat].isna(), column_cat] = "closed autocracy"

    # 2. Create variable for age group of liberal democracies
    column = "num_years_in_libdem"
    column_cat = f"{column}_cat"
    tb[column_cat] = pd.cut(tb[column], bins=bins, labels=labels)
    tb[column_cat] = Variable(tb[column_cat]).copy_metadata(tb.loc[:, column])

    ## Add additional categories
    tb[column_cat] = tb[column_cat].cat.add_categories(
        ["electoral democracy", "electoral autocracy", "closed autocracy"]
    )
    tb.loc[(tb["regime_row_owid"] == 2) & tb[column_cat].isna(), column_cat] = "electoral democracy"
    tb.loc[(tb["regime_row_owid"] == 1) & tb[column_cat].isna(), column_cat] = "electoral autocracy"
    tb.loc[(tb["regime_row_owid"] == 0) & tb[column_cat].isna(), column_cat] = "closed autocracy"

    return tb


def add_categories_women_in_parliament(tb: Table) -> Table:
    """Add categorical variable on the percentage of women in parliament."""
    bins = [
        -np.inf,
        0,
        10,
        20,
        30,
        40,
        50,
        np.inf,
    ]
    labels = [
        "0% women",
        "0-10% women",
        "10-20% women",
        "20-30% women",
        "30-40% women",
        "40-50% women",
        "50%+ women",
    ]
    column = "wom_parl_vdem"
    column_cat = f"{column}_cat"
    tb[column_cat] = pd.cut(tb[column], bins=bins, labels=labels)
    tb[column_cat] = Variable(tb[column_cat]).copy_metadata(tb.loc[:, column])

    return tb

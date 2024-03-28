import numpy as np
import pandas as pd
import yaml
from owid.catalog.tables import Table, concat

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# IMPUTE: We will infer indicator values for some countries based on their historical equivalences.
path = paths.directory / "countries_impute.yml"
COUNTRIES_IMPUTE = yaml.safe_load(path.read_text())

# Expected overlaps
IMPUTED_OVERLAPS_EXPECTED = {
    "Belgium": 2,
    "Finland": 54,
    "Kazakhstan": 1,
    "Pakistan": 24,
    "Serbia": 29,
    "Slovakia": 6,
    "Uzbekistan": 47,
}


def expand_observations(tb: Table) -> Table:
    """Expand to have a row per (year, country)."""
    # Column for index
    column_idx = ["country", "year"]

    # List of countries
    regions = set(tb["country"])

    # List of possible years
    years = np.arange(tb["year"].min(), tb["year"].max() + 1)

    # New index
    new_idx = pd.MultiIndex.from_product([regions, years], names=column_idx)

    # Add flag
    tb["vdem_obs"] = 1

    # Reset index
    tb = tb.set_index(column_idx, verify_integrity=True).reindex(new_idx).sort_index().reset_index()

    # Update flag
    tb["vdem_obs"] = tb["vdem_obs"].fillna(0)

    # Type of `year`
    tb["year"] = tb["year"].astype("int")
    return tb


def run(tb: Table) -> Table:
    """Impute values."""
    # %% Impute values from adjacent years
    # Conditions for Australia and the year 1900
    condition_australia_1900 = (tb["country"] == "Australia") & (tb["year"] == 1900)
    # Perform replacements (is this just based on 1899?)
    tb.loc[condition_australia_1900, "regime_row_owid"] = 3
    tb.loc[condition_australia_1900, "regime_redux_row_owid"] = 2
    tb.loc[condition_australia_1900, "regime_amb_row_owid"] = 8

    # The following are other candidates, but we discarded them because there are too many years missing.
    # - Honduras 1922-1933 (12 years missing)
    #   I favor no imputation because of 12 years of missing data, and the country may have met the criteria for democracy.
    # - Peru 1886-1891 (6 years missing)
    #   I favor no imputation because of six years of missing data, and even though one criterion for electoral autocracy is not met, the country may have met the criteria for democracy (if unlikely), thereby overriding the former.

    # %% Impute values based on historical equivalences
    tb_imputed = []
    for imp in COUNTRIES_IMPUTE:
        if "country_impute" in imp:
            # Create a new subdataframe, and checkd
            tb_ = tb.loc[
                (tb["country"] == imp["country_impute"])
                & (tb["year"] >= imp["year_min"])
                & (tb["year"] <= imp["year_max"])
            ].copy()
            # Sanity checks
            assert tb_.shape[0] > 0, f"No data found for {imp['country_impute']}"
            assert tb_["year"].max() == imp["year_max"], f"Missing years (max check) for {imp['country_impute']}"
            assert tb_["year"].min() == imp["year_min"], f"Missing years (min check) for {imp['country_impute']}"

            # Finish prep of subdataframe
            tb_ = tb_.rename(
                columns={
                    "country": "regime_imputed_country",
                }
            )
            tb_["regime_imputed"] = True
            tb_["country"] = imp["country"]

            # Check if there are overlaps; if so, and as expected, drop
            if not (merged := tb.merge(tb_, on=["country", "year"], how="inner")).empty:
                paths.log.info(f"Imputing data for {imp['country']}")
                assert imp["country"] in IMPUTED_OVERLAPS_EXPECTED, f"Unexpected overlap for {imp['country']}"
                assert (
                    len(merged) == IMPUTED_OVERLAPS_EXPECTED[imp["country"]]
                ), f"Unexpected overlap for {imp['country']}"
                # Drop
                tb_ = tb_[~tb_["year"].isin(merged["year"])]

            tb_imputed.append(tb_)

    tb = concat(tb_imputed + [tb], ignore_index=True)

    # Fill NaNs with False
    tb["regime_imputed"] = tb["regime_imputed"].fillna(False)

    # Check unique values
    tb.format(underscore=False).reset_index()

    return tb

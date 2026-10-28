"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

VERBOSE = False

# Indicator columns expected in the meadow table.
EXPECTED_COLUMNS = {
    "refugees",
    "returned_refugees",
    "asylum_seekers",
    "idps",
    "returned_idps",
    "stateless",
    "hst",
    "ooc",
    "oip",
}
# Minimum number of countries expected in each output table (counts in the 2025-07-03 version).
MIN_COUNTRIES = {"asylum": 196, "origin": 215}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("refugee_data")

    # Read table from meadow dataset.
    tb = ds_meadow.read("refugee_data")
    sanity_check_inputs(tb)

    # filter out data before data availability starts (s. https://www.unhcr.org/refugee-statistics/methodology/, "Data publication timeline")
    tb["asylum_seekers"] = tb["asylum_seekers"].where(tb["year"] >= 2000, pd.NA)
    tb["refugees"] = tb["refugees"].where(tb["year"] >= 1951, pd.NA)
    tb["returned_refugees"] = tb["returned_refugees"].where(tb["year"] >= 1965, pd.NA)
    tb["idps"] = tb["idps"].where(tb["year"] >= 1993, pd.NA)
    tb["stateless"] = tb["stateless"].where(tb["year"] >= 2004, pd.NA)
    # others of concern
    tb["ooc"] = tb["ooc"].where(tb["year"] >= 1997, pd.NA)
    # other people in need of international protection (category reported since 2018)
    # NOTE: People can move between oip and refugees (e.g. ~1M Afghans, mostly in Iran, were reclassified from
    # refugee-like situations to oip in 2025). Check for new reclassifications on every update and caveat them in the
    # description_key of both indicators.
    tb["oip"] = tb["oip"].where(tb["year"] >= 2018, pd.NA)
    tb["returned_idps"] = tb["returned_idps"].where(tb["year"] >= 1997, pd.NA)

    if VERBOSE:
        # there are 83 rows with refugees who have the same country of origin and asylum
        r_same = tb[(tb["country_of_origin"] == tb["country_of_asylum"]) & (tb["refugees"] > 0)]
        print(len(r_same), "rows with refugees who have the same country of origin and asylum")
        # there are 19 rows with asylum seekers who have the same country of origin and asylum:
        a_same = tb[(tb["country_of_origin"] == tb["country_of_asylum"]) & (tb["asylum_seekers"] > 0)]
        print(len(a_same), "rows with asylum seekers who have the same country of origin and asylum")
        # if country of origin and asylum are different idps are always 0 or nan
        idp = tb[(tb["country_of_origin"] != tb["country_of_asylum"]) & (tb["idps"] > 0)]
        print(len(idp), "rows with idps who have different country of origin and asylum")

    # set refugees and asylum seekers with the same country of origin and asylum as zero
    msk_r = (
        (tb["country_of_origin"] == tb["country_of_asylum"])
        & (tb["refugees"] > 0)
        & (tb["country_of_origin"] != "Unknown")
    )
    msk_a = (
        (tb["country_of_origin"] == tb["country_of_asylum"])
        & (tb["asylum_seekers"] > 0)
        & (tb["country_of_origin"] != "Unknown")
    )
    # set refugees and asylum seekers with the same country of origin and asylum to N/A instead of removing them, as they are likely to be idps
    tb.loc[msk_r, "refugees"] = pd.NA
    tb.loc[msk_a, "asylum_seekers"] = pd.NA

    # remove (for now) china after 2020 because of weird measurements
    msk_china = (tb["country_of_asylum"] == "China") & (tb["year"] > 2020)
    tb = tb[~(msk_china)]

    tb_origin = (
        tb.drop(columns=["country_of_asylum"])
        .groupby(["country_of_origin", "year"], observed=True)
        .sum(min_count=1)  # ty: ignore
        .reset_index()
    )
    tb_origin = tb_origin.rename(columns={"country_of_origin": "country"})

    tb_asylum = (
        tb.drop(columns=["country_of_origin"])
        .groupby(["country_of_asylum", "year"], observed=True)
        .sum(min_count=1)  # ty: ignore
        .reset_index()
    )
    tb_asylum = tb_asylum.rename(columns={"country_of_asylum": "country"})

    # harmonize countries
    tb_asylum = paths.regions.harmonize_names(
        tb_asylum, country_col="country", countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_origin = paths.regions.harmonize_names(
        tb_origin, country_col="country", countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # Add population to table
    tb_asylum = paths.regions.add_population(tb=tb_asylum)
    tb_origin = paths.regions.add_population(tb=tb_origin)

    # Calculate shares
    # asylum table
    tb_asylum["asylum_seekers_per_100k"] = tb_asylum["asylum_seekers"] / tb_asylum["population"] * 100_000
    tb_asylum["refugees_per_100k"] = tb_asylum["refugees"] / tb_asylum["population"] * 100_000
    tb_asylum["refugees_per_1000"] = tb_asylum["refugees"] / tb_asylum["population"] * 1000
    # stateless people always have "stateless" as country of origin
    tb_asylum["stateless_per_100k"] = tb_asylum["stateless"] / tb_asylum["population"] * 100_000

    # origin table
    tb_origin["asylum_seekers_per_100k"] = tb_origin["asylum_seekers"] / tb_origin["population"] * 100_000
    tb_origin["refugees_per_100k"] = (tb_origin["refugees"] / tb_origin["population"]) * 100_000
    tb_origin["refugees_per_1000"] = (tb_origin["refugees"] / tb_origin["population"]) * 1000
    # (returned) idps always have same origin and destination, so we only include them in origin table
    tb_origin["idps_per_100k"] = tb_origin["idps"] / tb_origin["population"] * 100_000
    tb_origin["returned_idps_per_100k"] = tb_origin["returned_idps"] / tb_origin["population"] * 100_000

    # drop idps from asylum table as we have them in the origin table
    tb_asylum = tb_asylum.drop(columns=["idps", "returned_idps", "population"])
    tb_origin = tb_origin.drop(columns=["population"])

    # Replace unknown countries with either "Unknown Origin" or "Unknown Destination"
    tb_asylum["country"] = tb_asylum["country"].replace("Unknown", "Unknown Destination")
    tb_origin["country"] = tb_origin["country"].replace("Unknown", "Unknown Origin")

    sanity_check_outputs(tb_asylum, tb_origin)

    # Improve table format.
    tb_asylum = tb_asylum.format(["country", "year"], short_name="refugee_data_asylum")
    tb_origin = tb_origin.format(["country", "year"], short_name="refugee_data_origin")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_asylum, tb_origin], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    columns = set(tb.columns) - {"country_of_origin", "country_of_asylum", "year"}
    assert columns == EXPECTED_COLUMNS, f"Unexpected meadow columns: {sorted(columns ^ EXPECTED_COLUMNS)}"
    assert not tb.duplicated(subset=["country_of_origin", "country_of_asylum", "year"]).any(), "Duplicate rows."
    assert (tb[list(EXPECTED_COLUMNS)].fillna(0) >= 0).all().all(), "Negative counts in meadow table."


def sanity_check_outputs(tb_asylum: Table, tb_origin: Table) -> None:
    for name, tb in [("asylum", tb_asylum), ("origin", tb_origin)]:
        assert not tb.duplicated(subset=["country", "year"]).any(), f"Duplicate (country, year) rows in {name} table."
        n_countries = tb["country"].nunique()
        assert n_countries >= MIN_COUNTRIES[name], f"Only {n_countries} countries in {name} table."
        rate_cols = [c for c in tb.columns if c.endswith(("_per_100k", "_per_1000"))]
        assert (tb[rate_cols].fillna(0) >= 0).all().all(), f"Negative rates in {name} table."
        # Refugees should not exceed the population of the country, except for Western Sahara as an origin: its
        # refugees (mostly in camps in Algeria) are not counted in the territory's population.
        msk = tb["refugees_per_1000"] > 1000
        if name == "origin":
            msk &= tb["country"] != "Western Sahara"
        assert not msk.any(), f"Refugee rate above 1,000 per 1,000 in {name} table: {set(tb.loc[msk, 'country'])}"

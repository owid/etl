"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

VERBOSE = False


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("refugee_data")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("refugee_data")

    # filter out data before data availability starts (s. https://www.unhcr.org/refugee-statistics/methodology/, "Data publication timeline")
    tb["asylum_seekers"] = tb["asylum_seekers"].where(tb["year"] >= 2000, pd.NA)
    tb["refugees"] = tb["refugees"].where(tb["year"] >= 1951, pd.NA)
    tb["returned_refugees"] = tb["returned_refugees"].where(tb["year"] >= 1965, pd.NA)
    tb["idps"] = tb["idps"].where(tb["year"] >= 1993, pd.NA)
    tb["stateless"] = tb["stateless"].where(tb["year"] >= 2004, pd.NA)
    # others of concern
    tb["ooc"] = tb["ooc"].where(tb["year"] >= 2018, pd.NA)
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

    # remove (for now) refugees and asylum seekers with the same country of origin and asylum
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
    tb = tb[~(msk_r)]
    tb = tb[~(msk_a)]

    # remove (for now) china after 2020 because of weird measurements
    msk_china = (tb["country_of_asylum"] == "China") & (tb["year"] > 2020)
    tb = tb[~(msk_china)]

    tb_origin = (
        tb.drop(columns=["country_of_asylum"])
        .groupby(["country_of_origin", "year"], observed=True)
        .sum(min_count=1)  # type: ignore
        .reset_index()
    )
    tb_origin = tb_origin.rename(columns={"country_of_origin": "country"})

    tb_asylum = (
        tb.drop(columns=["country_of_origin"])
        .groupby(["country_of_asylum", "year"], observed=True)
        .sum(min_count=1)  # type: ignore
        .reset_index()
    )
    tb_asylum = tb_asylum.rename(columns={"country_of_asylum": "country"})

    # harmonize countries
    tb_asylum = geo.harmonize_countries(
        df=tb_asylum,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    tb_origin = geo.harmonize_countries(
        df=tb_origin,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    # Add population to table
    tb_asylum = geo.add_population_to_table(tb=tb_asylum, ds_population=ds_population)
    tb_origin = geo.add_population_to_table(tb=tb_origin, ds_population=ds_population)

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

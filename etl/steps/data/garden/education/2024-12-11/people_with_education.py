"""Combine OECD historical education data (pre-1950) with Wittgenstein Centre data (1950+).

Produces a long-run series of the share of adults (15+) with and without formal education.
"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_wc = paths.load_dataset("wittgenstein_human_capital")
    tb_wc = ds_wc.read("by_sex_age_edu")

    ds_oecd = paths.load_dataset("oecd_education")
    tb_oecd = ds_oecd.read("oecd_education")

    #
    # Process data.
    #
    tb_oecd = make_oecd(tb_oecd)
    countries_oecd = set(tb_oecd["country"].unique())

    tb_wc = make_wc(tb_wc)
    countries_wc = set(tb_wc["country"].unique())

    # Combine tables.
    tb = pr.concat([tb_oecd, tb_wc], short_name="education")

    # Keep only countries present in both sources.
    countries = countries_oecd.intersection(countries_wc)
    tb = tb.loc[tb["country"].isin(countries)]

    tb = tb.format(["country", "year"], short_name="people_with_education")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_garden.save()


def make_oecd(tb):
    """Select OECD data for years before 1950 (Wittgenstein Centre covers 1950+)."""
    tb = tb.loc[
        tb["year"] < 1950, ["country", "year", "no_formal_education", "population_with_basic_education"]
    ].reset_index(drop=True)

    tb = tb.rename(
        columns={
            "no_formal_education": "no_basic_education",
            "population_with_basic_education": "basic_education",
        }
    )
    return tb


def make_wc(tb):
    """Extract the share with no education from Wittgenstein Centre (1950+, SSP2, age 15+, both sexes)."""
    tb = tb.loc[
        (tb["scenario"] == 2) & (tb["sex"] == "total") & (tb["age"] == "15+") & (tb["education"] == "no_education"),
        ["country", "year", "prop"],
    ]
    assert tb.groupby(["country", "year"]).size().max() == 1, "Only 1 row per country-year accepted."

    tb = tb.rename(columns={"prop": "no_basic_education"})
    tb["basic_education"] = 100 - tb["no_basic_education"]

    return tb

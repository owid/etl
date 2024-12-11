"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load Wittgenstein data
    ds_wc = paths.load_dataset("wittgenstein_human_capital")
    tb_wc = ds_wc.read("by_sex_age_edu")

    # Load garden historical OECD dataset.
    ds_oecd = paths.load_dataset("oecd_education")
    tb_oecd = ds_oecd.read("oecd_education")

    #
    # Process data.
    #
    # Prepare OECD
    tb_oecd = make_oecd(tb_oecd)
    countries_oecd = set(tb_oecd["country"].unique())

    # Prepare Wittgenstein Center
    tb_wc = make_wc(tb_wc)
    countries_wc = set(tb_wc["country"].unique())

    # Combine tables
    tb = pr.concat([tb_oecd, tb_wc], short_name="education")
    # Keep only relevant countries
    countries = countries_oecd.intersection(countries_wc)
    tb = tb.loc[tb["country"].isin(countries)]
    # Format
    tb = tb.format(["country", "year"], short_name="people_with_education")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_oecd(tb):
    # Filter the for years above 2020 (New Wittgenstein Center data starts at 2020)
    tb = tb.loc[
        tb["year"] < 1950, ["country", "year", "no_formal_education", "population_with_basic_education"]
    ].reset_index(drop=True)

    # Rename columns
    tb = tb.rename(
        columns={
            "no_formal_education": "no_basic_education",
            "population_with_basic_education": "basic_education",
        }
    )
    return tb


def make_wc(tb):
    tb = tb.loc[
        (tb["scenario"] == 2)
        # & (tb_wc["country"] == "World")
        & (tb["sex"] == "total")
        & (tb["age"] == "15+")
        & (tb["education"].isin(["no_education"])),
        ["country", "year", "prop"],
    ]
    assert tb.groupby(["country", "year"]).size().max() == 1, "Only 1 rows per country-year accepted"

    # Estimate "no formal education"
    tb = tb.rename(columns={"prop": "no_basic_education"})

    # Estimate "with basic education"
    tb["basic_education"] = 100 - tb["no_basic_education"]

    return tb

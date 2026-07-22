"""Combine census (1850-2000, decennial) and American Community Survey (2005-, annual) data on
the foreign-born population of the United States into one series."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def sanity_check(tb: Table) -> None:
    assert not tb["year"].duplicated().any(), "Duplicate years after combining census and ACS."
    assert (tb["foreign_born_population"] < tb["total_population"]).all(), "Foreign-born exceeds total population."
    share = tb.set_index("year")["share_foreign_born"]
    # Values printed in the census working paper.
    assert abs(share.loc[1890] - 14.8) < 0.05, "1890 share does not match the working paper."
    assert abs(share.loc[1970] - 4.7) < 0.05, "1970 share does not match the working paper."
    assert ((share > 4) & (share < 17)).all(), "Share outside the plausible historical range."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("us_foreign_born_population")
    tb_census = ds_meadow.read("census")
    tb_acs = ds_meadow.read("american_community_survey")

    #
    # Process data.
    #
    # The two sources do not overlap: census years run to 2000, ACS years start in 2005.
    tb = pr.concat([tb_census, tb_acs], ignore_index=True).sort_values("year")
    tb["share_foreign_born"] = tb["foreign_born_population"] / tb["total_population"] * 100
    tb["country"] = "United States"

    sanity_check(tb)

    tb = tb.drop(columns=["total_population"])

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

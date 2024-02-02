"""Load a meadow dataset and create a garden dataset.

- Before 1800: HYDE
- 1800 to 1938: Maddison
- 1938 to 1950: Maddison (only for Americas), HMD?, HYDE?
- 1950 to 2021: UN WPP
- 2022 to 2100: UN WPP projections
"""

import owid.catalog.processing as pr
from owid.catalog import Table
from utils import format_hyde, format_maddison, format_un

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Year separators between sources
YEAR_HYDE_TO_MADDISON = 1800
YEAR_MADDISON_TO_WPP = 1950


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load UN WPP dataset.
    ds_un = paths.load_dataset("un_wpp")
    tb_un = ds_un["population"].reset_index()
    # Load HYDE dataset.
    ds_hyde = paths.load_dataset("all_indicators")
    tb_hyde = ds_hyde["all_indicators"].reset_index()
    # Load Maddison dataset.
    ds_mad = paths.load_dataset("maddison_federico_paper")
    tb_mad = ds_mad["maddison_federico_paper"].reset_index()

    # Read table from meadow dataset.

    #
    # Process data.
    #
    tb_un = format_un(tb_un)
    tb_hyde = format_hyde(tb_hyde)
    tb_reference = tb_un.loc[tb_un["year"] == 1950, ["country", "population"]].set_index("country")
    tb_mad = format_maddison(tb_mad, tb_reference)

    # Concat tables
    tb = pr.concat([tb_hyde, tb_mad, tb_un], ignore_index=True, short_name=f"{paths.short_name}_original")

    tb = tb.pipe(select_source).astype(
        {
            "year": int,
            "population": "uint64",
        }
    )

    # Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def select_source(tb: Table) -> Table:
    """Select the best source for each country/year.

    The prioritisation scheme (i.e. what source is used/preferred) is:
    - For 1800 - 2100: WPP > Maddison > HYDE.
    - Prior to 1800: HYDE

    """
    paths.log.info("selecting source...")
    tb = tb.loc[tb["population"] > 0]

    # If a country has UN data, then remove all non-UN data after 1949
    has_un_data = set(tb.loc[tb["source"] == "unwpp", "country"])
    tb = tb.loc[~((tb["country"].isin(has_un_data)) & (tb["year"] >= YEAR_MADDISON_TO_WPP) & (tb["source"] != "unwpp"))]

    # If a country has Maddison data, then remove all non-Maddison data between 1800 and 1949
    has_maddison_data = set(tb.loc[tb["source"] == "maddison", "country"])
    tb = tb.loc[
        ~(
            (tb["country"].isin(has_maddison_data))
            & (tb["year"] >= YEAR_HYDE_TO_MADDISON)
            & (tb["year"] < YEAR_MADDISON_TO_WPP)
            & (tb["source"] != "maddison")
        )
    ]

    # Check if all countries have only one row per year
    _ = tb.set_index(["country", "year"], verify_integrity=True)

    # # map to source full names
    # tb["source"] = tb["source"].map(SOURCES_NAMES)
    return tb

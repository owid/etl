"""Load a meadow dataset and create a garden dataset."""

from typing import List

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    # Defra data for England and Scotland
    snap_meadow_defra = paths.load_snapshot("forest_share", namespace="defra")
    # FAO data for England and Scotland
    snap_meadow_fao = paths.load_snapshot("forest_share", namespace="fao")
    # Forest cover data for  Scotland
    snap_meadow_forest_research = paths.load_snapshot("forest_share", namespace="forest_research")
    # Forest cover data for France
    snap_meadow_france = paths.load_snapshot("france_forest_share", namespace="papers")
    # Forest cover data for Japan
    snap_meadow_japan = paths.load_snapshot("japan_forest_share", namespace="papers")
    # Forest cover data for Taiwan
    snap_meadow_taiwan = paths.load_snapshot("taiwan_forest_share", namespace="papers")
    # Forest cover data for the Scotland
    snap_meadow_scotland = paths.load_snapshot("mather_2004", namespace="papers")
    # Forest cover data for Costa Rica
    snap_meadow_costa_rica = paths.load_snapshot("kleinn_2000", namespace="papers")
    # Forest research data for South Korea
    snap_meadow_south_korea = paths.load_snapshot("soo_bae_et_al_2012", namespace="papers")
    # Forest research data for USA
    snap_meadow_usa = paths.load_snapshot("forest_share", namespace="usda_fs")
    # Forest data for China
    snap_meadow_china = paths.load_snapshot("he_2025", namespace="papers")
    # More recent forest data for England and Scotland - from the Scottish Government
    snap_meadow_sg = paths.load_snapshot("scottish_government", namespace="papers")

    # FAO Forest Resource Assessment (FRA) 2025 data
    ds_garden_fra = paths.load_dataset("fra")
    # WDI data to get land area for each country
    ds_garden_wdi = paths.load_dataset("wdi")
    # Read table from meadow dataset.
    tb_defra = snap_meadow_defra.read()
    tb_fao = snap_meadow_fao.read()
    tb_forest_research = snap_meadow_forest_research.read()
    tb_france = snap_meadow_france.read()
    tb_japan = snap_meadow_japan.read()
    tb_taiwan = snap_meadow_taiwan.read()
    tb_scotland = snap_meadow_scotland.read()
    tb_costa_rica = snap_meadow_costa_rica.read()
    tb_south_korea = snap_meadow_south_korea.read()
    tb_usa = snap_meadow_usa.read()
    tb_china = snap_meadow_china.read()
    tb_sg = snap_meadow_sg.read()
    tb_fra = ds_garden_fra["fra"].reset_index()
    tb_fra["source"] = "Forest Resource Assessment (FRA) 2025"
    tb_wdi = ds_garden_wdi["wdi"].reset_index()
    tb_fra = calculate_fra_forest_share(tb_fra=tb_fra, tb_wdi=tb_wdi)
    # Concatenate tables.
    tb = pr.concat(
        [
            tb_defra,
            tb_fao,
            tb_forest_research,
            tb_france,
            tb_japan,
            tb_taiwan,
            tb_scotland,
            tb_costa_rica,
            tb_south_korea,
            tb_usa,
            tb_china,
            tb_sg,
        ]
    )

    tb_com = combine_datasets(
        tb_a=tb, tb_b=tb_fra, table_name="forest_share", preferred_source="Forest Resource Assessment (FRA) 2025"
    )
    tb_com = tb_com.drop(columns=["source"])
    #

    # Improve table format.
    tb_com = tb_com.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_com], default_metadata=snap_meadow_defra.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_fra_forest_share(tb_fra: Table, tb_wdi: Table) -> Table:
    """
    Combine the forest area variable (hectares) from FRA (2025) with the land area
    data (sq. km) from WDI to calculate the share of land covered in forest for each country.

    Return just country, year, forest share and source
    """
    tb_fra = tb_fra[["country", "year", "_1a_forestarea", "source"]]
    # Select out the total land area variable: ag_lnd_totl_k2
    tb_wdi = tb_wdi[["country", "year", "ag_lnd_totl_k2"]]
    # Strangely this land area value changes in 1991 and 2011, with the dissolution of the soviet union and creation of south sudan
    tb_wdi = tb_wdi.dropna()
    tb_wdi = tb_wdi[tb_wdi["year"] == max(tb_wdi["year"])]
    tb_wdi = tb_wdi.drop(columns="year")
    # Manually add in French Guiana and Western Sahara, the value for France (547557 km2) already seems to exclude french guiana, so no need to change that
    tb_wdi.append(pd.DataFrame({"country": ["French Guiana", "Western Sahara"], "ag_lnd_totl_k2": [83534, 272000]}))
    # convert from square km to hectares
    tb_wdi["ag_lnd_totl_k2"] *= 100
    tb = pr.merge(tb_fra, tb_wdi, on=["country"])
    tb["forest_share"] = (tb["_1a_forestarea"] / tb["ag_lnd_totl_k2"]) * 100
    tb = tb[["country", "year", "forest_share", "source"]]

    return tb


def combine_datasets(tb_a: Table, tb_b: Table, table_name: str, preferred_source: str) -> Table:
    """
    Combine two tables with a preference for one source of data.
    """
    tb_combined = pr.concat([tb_a, tb_b], short_name=table_name).sort_values(
        ["country", "year", "source"], ignore_index=True
    )
    assert any(tb_combined["source"] == preferred_source), "Preferred source not in table!"
    tb_combined = remove_duplicates(
        tb_combined,
        preferred_source=preferred_source,
        dimensions=["country", "year"],
    )

    return tb_combined


def remove_duplicates(tb: Table, preferred_source: str, dimensions: List[str]) -> Table:
    """
    Removing rows where there are overlapping years with a preference for FRA data.

    """
    assert any(tb["source"] == preferred_source)
    tb = tb.copy(deep=True)
    duplicate_rows = tb.duplicated(subset=dimensions, keep=False)

    tb_no_duplicates = tb[~duplicate_rows]

    tb_duplicates = tb[duplicate_rows]

    tb_duplicates_removed = tb_duplicates[tb_duplicates["source"] == preferred_source]

    tb = pr.concat([tb_no_duplicates, tb_duplicates_removed], ignore_index=True)

    assert len(tb[tb.duplicated(subset=dimensions, keep=False)]) == 0, "Duplicates still in table!"

    return tb

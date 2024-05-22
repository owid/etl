"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog.tables import Table, concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #

    # Retieve data from Gapminder
    snap = paths.load_snapshot("eiu_gapminder.csv")
    tb_gm = snap.read()

    # Retrieve data from EIU (single year reports)
    shortnames = [
        # "eiu_gapminder",
        "eiu_2021",
        "eiu_2022",
        "eiu_2023",
    ]
    tbs = []
    for name in shortnames:
        snap = paths.load_snapshot(f"{name}.csv")
        tb = snap.read()
        tbs.append(tb)

    # Correct data by Gapminder
    ## Gapminder multiplies all values by ten.
    cols = [
        "democracy_eiu",
        "elect_freefair_eiu",
        "funct_gov_eiu",
        "pol_part_eiu",
        "dem_culture_eiu",
        "civlib_eiu",
    ]
    tb_gm[cols] = tb_gm[cols] / 10

    ## Add missing data
    tb_gm = add_datapoints(tb_gm)

    # Concatenate all tables.
    tbs.append(tb_gm)
    tb = concat(tbs, ignore_index=True, short_name="eiu")

    #
    # Process data.
    #
    tb = tb.rename(
        columns={
            "country_name": "country",
        }
    )

    tb["rank_eiu"] = tb["rank_eiu"].str.replace("=", "")
    tb["rank_eiu"] = tb["rank_eiu"].astype("float")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def add_datapoints(tb: Table) -> Table:
    """Add missing datapoints in Gapminder data."""
    # Define records
    records = [
        {
            "country_name": "Algeria",
            "democracy_eiu": 3.77,
            "elect_freefair_eiu": 3.08,
            "funct_gov_eiu": 2.5,
            "pol_part_eiu": 4.44,
            "dem_culture_eiu": 5,
            "civlib_eiu": 3.82,
        },
        {
            "country_name": "Iran",
            "democracy_eiu": 2.2,
            "elect_freefair_eiu": 0,
            "funct_gov_eiu": 2.5,
            "pol_part_eiu": 3.89,
            "dem_culture_eiu": 3.13,
            "civlib_eiu": 1.47,
        },
        {
            "country_name": "Lithuania",
            "democracy_eiu": 7.13,
            "elect_freefair_eiu": 9.58,
            "funct_gov_eiu": 6.07,
            "pol_part_eiu": 5.56,
            "dem_culture_eiu": 5.63,
            "civlib_eiu": 8.82,
        },
        {
            "country_name": "Ukraine",
            "democracy_eiu": 5.81,
            "elect_freefair_eiu": 8.25,
            "funct_gov_eiu": 2.71,
            "pol_part_eiu": 7.22,
            "dem_culture_eiu": 5,
            "civlib_eiu": 5.88,
        },
    ]
    tb_ext = Table.from_records(records).assign(year=2020)

    # Add to main table
    tb = concat([tb, tb_ext], ignore_index=True, short_name=tb.m.short_name)

    return tb

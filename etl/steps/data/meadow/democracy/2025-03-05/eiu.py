"""Load a snapshot and create a meadow dataset."""

from owid.catalog.tables import Table, concat

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve data from Gapminder
    snap = paths.load_snapshot("eiu_gapminder.csv")
    tb_gm = snap.read(safe_types=False)

    # Retrieve data from EIU (single year reports)
    shortnames = [
        "eiu_2021",
        "eiu_2022",
        "eiu_2023",
        "eiu_2024",
    ]
    tbs = []
    for name in shortnames:
        snap = paths.load_snapshot(f"{name}.csv")
        tb = snap.read(safe_types=False)
        tbs.append(tb)

    # Correct data by Gapminder
    tb_gm = scale_indicators_gm(tb_gm)

    ## Add missing data in Gapminder
    tb_gm = add_datapoints(tb_gm)

    # Concatenate all tables.
    tbs.append(tb_gm)
    tb = concat(tbs, ignore_index=True, short_name="eiu")

    #
    # Process data.
    #
    # Rename country column
    tb = tb.rename(
        columns={
            "country_name": "country",
        }
    )

    # Drop rows if country is NA
    tb = tb.dropna(subset=["country"])

    # Fix type of rank
    tb["rank_eiu"] = tb["rank_eiu"].str.replace("=", "")
    tb["rank_eiu"] = tb["rank_eiu"].astype("UInt16")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def scale_indicators_gm(tb):
    """Gapminder multiplies all values by ten."""
    cols = [
        "democracy_eiu",
        "elect_freefair_eiu",
        "funct_gov_eiu",
        "pol_part_eiu",
        "dem_culture_eiu",
        "civlib_eiu",
    ]
    tb[cols] = tb[cols] / 10

    return tb


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

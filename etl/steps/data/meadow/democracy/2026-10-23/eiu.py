"""Load a snapshot and create a meadow dataset.

The data comes from several snapshots:
- 2006-2020: Gapminder's compilation of EIU's yearly reports.
- 2021-2024: one CSV per year, transcribed from EIU's yearly reports (with the five category scores).
- 2025 onwards: a spreadsheet EIU provided to us (overall score, rank and regime type only).
"""

from owid.catalog import processing as pr
from owid.catalog.tables import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# First year taken from the EIU spreadsheet. Earlier years come from the transcribed reports.
# NOTE: Checked against EIU's 2008, 2018, 2019 and 2021 reports, the spreadsheet's history has errors: Ireland and Italy
# swapped in 2018-2021, Sweden and Switzerland swapped in 2021, and a 2008 column that repeats the 2011 values. Re-check it
# at every update before using more of it.
YEAR_XLSX_MIN = 2025
# Series in the EIU spreadsheet, and the columns we map them to.
XLSX_SERIES = {
    "Overall score (0-10)": "democracy_eiu",
    "Rank": "rank_eiu",
    "Regime type": "regime_type_eiu",
}


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
    year_reports_max = max(tb["year"].max() for tb in tbs)

    # Retrieve data from EIU (spreadsheet with all editions)
    snap = paths.load_snapshot("eiu.xlsx")
    tb_xlsx = snap.read(safe_types=False)

    # Correct data by Gapminder
    tb_gm = scale_indicators_gm(tb_gm)

    ## Add missing data in Gapminder
    tb_gm = add_datapoints(tb_gm)

    # Reshape the EIU spreadsheet to long format, and keep only the years not covered by the yearly reports.
    tb_xlsx = reshape_xlsx(tb_xlsx)
    assert tb_xlsx["year"].min() > year_reports_max, "EIU spreadsheet years overlap with the yearly reports."

    # Concatenate all tables.
    tbs.extend([tb_gm, tb_xlsx])
    tb = pr.concat(tbs, ignore_index=True, short_name="eiu")

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
    tb_ext = pr.read_from_records(records).assign(year=2020)

    # Add to main table
    tb = pr.concat([tb, tb_ext], ignore_index=True, short_name=tb.m.short_name)

    return tb


def reshape_xlsx(tb: Table) -> Table:
    """Reshape the EIU spreadsheet (one row per country and series, one column per year) to one row per country-year."""
    # Check the spreadsheet still has the expected series.
    assert set(tb["Series"]) == set(XLSX_SERIES), f"Unexpected series in EIU spreadsheet: {set(tb['Series'])}"

    # Wide to long, then one column per series.
    year_columns = [col for col in tb.columns if str(col).isdigit()]
    tb = tb.melt(id_vars=["Country", "Series"], value_vars=year_columns, var_name="year", value_name="value")
    tb = tb.pivot(index=["Country", "year"], columns="Series", values="value").reset_index()
    tb.columns.name = None
    tb = tb.rename(columns={"Country": "country_name", **XLSX_SERIES})
    tb["year"] = tb["year"].astype(int)

    # Keep only the years we take from the spreadsheet.
    tb = tb.loc[tb["year"] >= YEAR_XLSX_MIN].reset_index(drop=True)

    # Set types (rank as string, to match the yearly reports, where ties are marked with "=").
    tb["democracy_eiu"] = tb["democracy_eiu"].astype(float)
    tb["rank_eiu"] = tb["rank_eiu"].astype(int).astype(str)
    tb["regime_type_eiu"] = tb["regime_type_eiu"].astype("string")

    return tb

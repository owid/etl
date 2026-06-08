"""Load a meadow dataset and create a garden dataset."""

import re

import structlog

from etl.helpers import PathFinder

log = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def check_annual_vs_decade_totals(tb_annual, tb_decades, verbose: bool = False) -> None:
    """Check that annual catch totals, when aggregated by decade, match the decade table totals.

    tb_annual has columns: [year, species1, species2, ...]
    tb_decades has columns: [species, _YYYY_YY, ...]  (one column per decade range)
    """
    non_data_cols = {"species", "country", "hemisphere", "total", "year"}
    species_cols = [c for c in tb_annual.columns if c not in non_data_cols]
    decade_cols = [c for c in tb_decades.columns if c not in non_data_cols]

    # tb_decades["species"] holds original names (e.g. "Blue Whale") while tb_annual
    # has catalog-sanitized column names (e.g. "blue_whale"). Build a lookup map.
    def _normalize(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")

    species_col_map = {_normalize(c): c for c in species_cols}

    # Group annual data by decade (decade_start = year // 10 * 10)
    tb_annual = tb_annual.copy()
    tb_annual["decade_start"] = (tb_annual["year"] // 10) * 10
    annual_by_decade = tb_annual.groupby("decade_start")[species_cols].sum()

    mismatches = []
    for col in decade_cols:
        # Column names are like _1900_09 → decade start is 1900
        decade_start = int(str(col).lstrip("_")[:4])
        if decade_start not in annual_by_decade.index:
            log.warning(
                "check_annual_vs_decade.no_annual_data",
                decade=col,
                decade_start=decade_start,
            )
            continue
        annual_row = annual_by_decade.loc[decade_start]
        for _, row in tb_decades.iterrows():
            species_raw = row["species"]
            annual_col = species_col_map.get(_normalize(str(species_raw)))
            if annual_col is None:
                continue
            decade_val = row[col]
            annual_val = annual_row[annual_col]
            both_nan = (decade_val != decade_val) and (annual_val != annual_val)  # NaN check
            diff = abs(decade_val - annual_val) if not both_nan else 0
            if verbose:
                log.info(
                    "check_annual_vs_decade",
                    species=species_raw,
                    decade=col,
                    decade_val=decade_val,
                    annual_sum=annual_val,
                    diff=diff,
                    ok=diff <= 1,
                )
            if not both_nan and diff > 1:
                mismatches.append(
                    f"  species='{species_raw}', decade='{col}': decade_table={decade_val}, annual_sum={annual_val}"
                )

    if mismatches:
        # log all mismatches
        for mismatch in mismatches:
            log.warning("check_annual_vs_decade_mismatch", message=mismatch)

    if not mismatches:
        log.info("check_annual_vs_decade_totals.ok", message="Decade totals match annual data")


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("historic_whaling")

    # Read table from meadow dataset.
    tb_decades = ds_meadow.read("historic_whaling_decades")
    tb_annual = ds_meadow.read("historic_whaling_annual")

    # add southern and northern hemisphere data
    tb_decades = tb_decades.groupby(["species"]).sum().reset_index().drop(columns=["hemisphere"])

    tb_annual = tb_annual.groupby(["year"]).sum().reset_index().drop(columns=["hemisphere"])
    tb_annual["country"] = "World"

    # Decade totals don't match annual total because of illegal soviet whaling data (which is only available on decadal basis). Log a warning but continue processing.
    check_annual_vs_decade_totals(tb_annual, tb_decades)

    # transpose decade table to match annual table
    origins_decades = tb_decades["_1900_09"].m.origins.copy()
    tb_decades = tb_decades.set_index("species").transpose().reset_index()
    tb_decades = tb_decades.rename(columns={"index": "decade"}).replace(
        {
            "_1900_09": "1900-1909",
            "_1910_19": "1910-1919",
            "_1920_29": "1920-1929",
            "_1930_39": "1930-1939",
            "_1940_49": "1940-1949",
            "_1950_59": "1950-1959",
            "_1960_69": "1960-1969",
            "_1970_79": "1970-1979",
            "_1980_89": "1980-1989",
            "_1990_99": "1990-1999",
        }
    )
    tb_decades["country"] = "World"

    for col in tb_decades.columns:
        if col not in ["decade", "country"]:
            tb_decades[col].m.origins = origins_decades

    #

    # Improve table format.
    tb_decades = tb_decades.format(["decade", "country"])
    tb_annual = tb_annual.format(["year", "country"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_decades, tb_annual], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

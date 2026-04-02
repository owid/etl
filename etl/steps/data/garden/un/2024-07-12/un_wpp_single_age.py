"""Garden step for UN WPP population with single-year age groups (no binning).

This is a minimal version of the un_wpp garden step that keeps the original
single-year age breakdown (0, 1, 2, …, 99, 100+) rather than aggregating into
5-year bins.  Only the population and median_age tables are produced.
"""

import pathlib

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

YEAR_SPLIT = 2024
COLUMNS_INDEX = ["country", "year", "sex", "age", "variant"]

# Reuse country mappings from the existing un_wpp garden step
_HERE = pathlib.Path(__file__).parent
COUNTRIES_FILE = _HERE / "un_wpp.countries.json"
EXCLUDED_COUNTRIES_FILE = _HERE / "un_wpp.excluded_countries.json"


def run() -> None:
    ds_meadow = paths.load_dataset("un_wpp")

    tb_population = ds_meadow.read("population")
    tb_median_age = ds_meadow.read("median_age")

    tb_population = process_population(tb_population)
    tb_median_age = process_median_age(tb_median_age)

    ds_garden = paths.create_dataset(
        tables=[tb_population, tb_median_age],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()


def process_population(tb: Table) -> Table:
    # Scale: meadow stores thousands
    tb["population"] = (tb["population"] * 1000).astype(int)

    # Keep July (mid-year) only, then drop month column
    tb = tb.loc[tb["month"] == "July"].drop(columns=["month"])

    # Harmonize country names
    tb = paths.regions.harmonize_names(
        tb,
        countries_file=COUNTRIES_FILE,
        excluded_countries_file=EXCLUDED_COUNTRIES_FILE,
    )

    # Lowercase variants
    tb["variant"] = tb["variant"].replace(
        {
            "Medium": "medium",
            "Low": "low",
            "High": "high",
            "Constant fertility": "constant_fertility",
        }
    )

    # Years before YEAR_SPLIT are historical estimates
    tb["variant"] = tb["variant"].astype("string")
    tb.loc[tb["year"] < YEAR_SPLIT, "variant"] = "estimates"

    # Keep only single-year numeric ages (0–99) and 100+
    tb = tb.loc[tb["age"].isin([str(i) for i in range(100)] + ["100+"])]

    tb = tb.format(COLUMNS_INDEX, short_name="population")
    return tb


def process_median_age(tb: Table) -> Table:
    tb = paths.regions.harmonize_names(
        tb,
        countries_file=COUNTRIES_FILE,
        excluded_countries_file=EXCLUDED_COUNTRIES_FILE,
    )

    tb["variant"] = tb["variant"].replace(
        {
            "Estimates": "estimates",
            "Medium": "medium",
            "Low": "low",
            "High": "high",
        }
    )
    tb["variant"] = tb["variant"].astype("string")

    tb = tb.format(COLUMNS_INDEX, short_name="median_age")
    return tb

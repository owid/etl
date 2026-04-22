"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the new data, and how to rename them.
COLUMNS = {
    "year": "year",
    "enriched": "number_of_eggs_from_enriched_cages",
    "barn": "number_of_eggs_from_barns",
    "free_range": "number_of_eggs_from_non_organic_free_range_farms",
    "organic": "number_of_eggs_from_organic_free_range_farms",
    "total": "number_of_eggs_all",
}

# Columns in the old meadow data that map to the same garden columns.
COLUMNS_OLD = {
    "year": "year",
    "enriched": "number_of_eggs_from_enriched_cages",
    "barn": "number_of_eggs_from_barns",
    "free_range": "number_of_eggs_from_non_organic_free_range_farms",
    "organic": "number_of_eggs_from_organic_free_range_farms",
}


def estimate_hens_by_housing_type(tb: Table, tb_hens: Table) -> Table:
    """Estimate the number of hens in each housing type.

    Uses total laying hens from the June Survey of Agriculture and the share of eggs
    produced in each housing type as a proxy, assuming equal laying rates across systems.
    """
    tb = tb.merge(tb_hens[["year", "total_laying_hens"]], on="year", how="left")

    total_eggs = tb["number_of_eggs_all"]
    tb["number_of_hens_in_cages"] = (
        (tb["total_laying_hens"] * tb["number_of_eggs_from_enriched_cages"] / total_eggs).round().astype("Int64")
    )
    tb["number_of_hens_in_barns"] = (
        (tb["total_laying_hens"] * tb["number_of_eggs_from_barns"] / total_eggs).round().astype("Int64")
    )
    tb["number_of_hens_free_range"] = (
        (tb["total_laying_hens"] * tb["number_of_eggs_from_non_organic_free_range_farms"] / total_eggs)
        .round()
        .astype("Int64")
    )
    tb["number_of_hens_organic"] = (
        (tb["total_laying_hens"] * tb["number_of_eggs_from_organic_free_range_farms"] / total_eggs)
        .round()
        .astype("Int64")
    )
    tb["number_of_hens_cage_free"] = (
        tb["number_of_hens_in_barns"] + tb["number_of_hens_free_range"] + tb["number_of_hens_organic"]
    )
    tb = tb.drop(columns=["total_laying_hens"])

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load new meadow dataset.
    ds_meadow = paths.load_dataset("uk_egg_statistics", version="2026-04-16")
    tb = ds_meadow.read("uk_egg_statistics")

    # Load old meadow dataset (has barn/organic data for 2006-2011 that is suppressed in the new release).
    ds_meadow_old = paths.load_dataset("uk_egg_statistics", version="2023-08-01")
    tb_old = ds_meadow_old.read("uk_egg_statistics")

    # Load total laying hen counts from the June Survey of Agriculture.
    ds_hens = paths.load_dataset("uk_livestock_populations")
    tb_hens = ds_hens.read("uk_livestock_populations").reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")
    tb_old = tb_old[list(COLUMNS_OLD)].rename(columns=COLUMNS_OLD, errors="raise")

    # Convert million dozens of eggs to eggs.
    egg_columns = [col for col in COLUMNS.values() if col != "year"]
    for column in egg_columns:
        tb[column] = tb[column].astype("Float64") * 12e6

    egg_columns_old = [col for col in COLUMNS_OLD.values() if col != "year"]
    for column in egg_columns_old:
        tb_old[column] = tb_old[column].astype("Float64") * 12e6

    # Backfill suppressed barn/organic values from the old release. The new release marks these as [c] (suppressed for disclosure) for 2006-2011, but the old release had values that are consistent with surrounding years.
    backfill_columns = ["number_of_eggs_from_barns", "number_of_eggs_from_organic_free_range_farms"]
    tb = tb.merge(tb_old[["year"] + backfill_columns], on="year", how="left", suffixes=("", "_old"))
    for col in backfill_columns:
        tb[col] = tb[col].fillna(tb[f"{col}_old"])
        tb = tb.drop(columns=[f"{col}_old"])

    # Add a country column.
    tb["country"] = "United Kingdom"

    # Estimate hens by housing type from total hen counts and egg share by housing type.
    tb = estimate_hens_by_housing_type(tb, tb_hens)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save new garden dataset.
    ds_garden.save()

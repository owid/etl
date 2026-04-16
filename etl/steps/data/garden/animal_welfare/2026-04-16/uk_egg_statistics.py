"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load new meadow dataset.
    ds_meadow = paths.load_dataset("uk_egg_statistics", version="2026-04-16")
    tb = ds_meadow["uk_egg_statistics"].reset_index()

    # Load old meadow dataset (has barn/organic data for 2006-2011 that is suppressed in the new release).
    ds_meadow_old = paths.load_dataset("uk_egg_statistics", version="2023-08-01")
    tb_old = ds_meadow_old["uk_egg_statistics"].reset_index()

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

    old_egg_columns = [col for col in COLUMNS_OLD.values() if col != "year"]
    for column in old_egg_columns:
        tb_old[column] = tb_old[column].astype("Float64") * 12e6

    # Backfill suppressed barn/organic values from the old release.
    # The new release marks these as [c] (suppressed for disclosure) for 2006-2011,
    # but the old release had values that are consistent with surrounding years.
    backfill_columns = ["number_of_eggs_from_barns", "number_of_eggs_from_organic_free_range_farms"]
    tb = tb.merge(tb_old[["year"] + backfill_columns], on="year", how="left", suffixes=("", "_old"))
    for col in backfill_columns:
        tb[col] = tb[col].fillna(tb[f"{col}_old"])
        tb = tb.drop(columns=[f"{col}_old"])

    # Add a country column.
    tb["country"] = "United Kingdom"

    # Set an appropriate index and sort conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

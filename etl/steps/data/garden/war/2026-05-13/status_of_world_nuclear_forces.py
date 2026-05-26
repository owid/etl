"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to read from the data, and how to rename them.
COLUMNS = {
    "country": "country",
    "year": "year",
    "deployed_strategic": "deployed_strategic",
    "deployed_nonstrategic": "deployed_nonstrategic",
    "reserve_nondeployed": "reserve_nondeployed",
    "military_stockpile__a": "stockpile",
    "total_inventory__b": "inventory",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("status_of_world_nuclear_forces")
    tb = ds_meadow.read("status_of_world_nuclear_forces")

    #
    # Process data.
    #
    # Rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Remove annotations and thousand separators from data values, and set an appropriate type.
    for column in tb.drop(columns=["country", "year"]).columns:
        tb[column] = tb[column].str.replace(r"\D", "", regex=True).astype(float)

    # Looking at the original dashboard, it seems that missing values are shown as zeros.
    # https://public.tableau.com/app/profile/kate.kohn/viz/EstimatedGlobalNuclearWarheadInventories2021/Dashboard1
    cols = [c for c in tb.columns if c not in ["country", "year"]]
    tb[cols] = tb[cols].fillna(0)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb, countries_file=paths.country_mapping_path)

    # Sanity check: the per-category breakdown (deployed_strategic + deployed_nonstrategic + reserve_nondeployed)
    # should never exceed the total stockpile. FAS treats `stockpile` as authoritative and sometimes publishes a
    # partial breakdown (e.g. India 2026: stockpile 190 vs. 0+0+178 = 178), so we don't require strict equality —
    # but the breakdown exceeding stockpile would be a parsing error.
    breakdown_sum = tb[["deployed_strategic", "deployed_nonstrategic", "reserve_nondeployed"]].sum(axis=1)
    error = "Per-category warhead breakdown exceeds the stockpile total for some country-year."
    assert (breakdown_sum <= tb["stockpile"]).all(), error

    # Add column for retired nuclear weapons, which is the total inventory minus the stockpile.
    tb["retired"] = tb["inventory"] - tb["stockpile"]

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()

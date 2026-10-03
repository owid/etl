"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Columns with hen counts (all columns except country and year).
DATA_COLUMNS = ["not_enriched_cage", "enriched_cage", "free_range", "barn", "organic", "total"]


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    assert (tb[DATA_COLUMNS].fillna(0) >= 0).all().all(), "Negative hen counts found."
    # In the source file, the reported components always add up exactly to the reported total.
    components_sum = tb[DATA_COLUMNS[:-1]].fillna(0).sum(axis=1).astype("Int64")
    assert (components_sum == tb["total"])[tb["total"].notna()].all(), "Components do not add up to total."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("laying_hens_keeping_eu")
    tb = ds_meadow.read("laying_hens_keeping_eu")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb)

    # Remove rows of country-years for which the source reports no data at all.
    tb = tb.dropna(subset=DATA_COLUMNS, how="all").reset_index(drop=True)

    # Remove two known-erroneous rows (Belgium 2025, Greece 2013) — see laying_hens_keeping_eu.corrections.yml.
    tb = paths.apply_corrections(tb)

    # Run sanity checks on outputs.
    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

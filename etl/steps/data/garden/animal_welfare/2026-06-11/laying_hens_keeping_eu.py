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

    ####################################################################################################################
    # In the current snapshot, the Belgium 2025 row is spurious: the barn value is a copy of the
    # free range value, which makes the total inconsistent with the EU eggs dashboard (where the same
    # notification is reported with a ~5.27 million barn value and a ~11.34 million total):
    # https://agriculture.ec.europa.eu/document/download/9bdf9842-1eb6-41a2-8845-49738b812b2b_en?filename=eggs-dashboard_en.pdf
    # So we remove the 2025 row.
    error = "Belgium 2025 row may have been fixed in the source file; remove this workaround."
    belgium_2025 = tb[(tb["country"] == "Belgium") & (tb["year"] == 2025)]
    assert len(belgium_2025) == 1, error
    assert (belgium_2025["barn"] == belgium_2025["free_range"]).all(), error
    tb = tb.drop(index=belgium_2025.index).reset_index(drop=True)

    # The Greece 2013 row reports a total of just 23,166 hens, whereas Greece's other years are
    # around 4-5 million; it is most likely an incomplete notification, so we remove it.
    error = "Greece 2013 row may have been fixed in the source file; remove this workaround."
    greece_2013 = tb[(tb["country"] == "Greece") & (tb["year"] == 2013)]
    assert len(greece_2013) == 1, error
    assert (greece_2013["total"] < 100_000).all(), error
    tb = tb.drop(index=greece_2013.index).reset_index(drop=True)
    ####################################################################################################################

    # Run sanity checks on outputs.
    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

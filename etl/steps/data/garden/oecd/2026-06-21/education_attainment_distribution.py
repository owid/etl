"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def sanity_check_inputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    assert tb["share_tertiary_education"].min() >= 0, "Negative share found."
    assert tb["share_tertiary_education"].max() <= 100, "Share exceeds 100%."
    assert len(tb) > 100, f"Unexpectedly few rows: {len(tb)}."


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.empty, "Output table is empty."
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."


def run() -> None:
    ds_meadow = paths.load_dataset("education_attainment_distribution")
    tb = ds_meadow["education_attainment_distribution"].reset_index()

    sanity_check_inputs(tb)

    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    sanity_check_outputs(tb)

    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()

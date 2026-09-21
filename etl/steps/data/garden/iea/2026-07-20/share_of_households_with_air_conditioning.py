"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def sanity_check_inputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows found."
    assert tb["year"].between(2000, 2030).all(), "Year outside the expected range."
    share = tb["share_households_ac"]
    assert share.notna().all(), "Unexpected missing values in the air-conditioning share."
    assert share.between(0, 100).all(), "Air-conditioning share outside the 0-100 range."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    assert not tb.index.duplicated().any(), "Duplicate index entries in the output table."


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("share_of_households_with_air_conditioning")
    tb = ds_meadow["share_of_households_with_air_conditioning"].reset_index()

    #
    # Process data.
    #
    sanity_check_inputs(tb)

    # Give the indicator a descriptive, self-explanatory name.
    tb = tb.rename(columns={"share_households_ac": "share_of_households_with_air_conditioning"})

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format(["country", "year"])

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

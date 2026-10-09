"""Load a snapshot and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Map the producer's column names to the short names used since the 2024 release.
COLUMNS = {
    "Entity": "country",
    "Year": "year",
    "Number of species included in the LPI": "species_in_lpi",
    "Number of species not included in the LPI": "species_not_in_lpi",
    "Number of species in the taxonomic group": "species_total",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("living_planet_index_completeness.csv")
    tb = snap.read()

    #
    # Process data.
    #
    assert set(tb.columns) == set(COLUMNS), f"Unexpected columns: {set(tb.columns) ^ set(COLUMNS)}"
    tb = tb.rename(columns=COLUMNS, errors="raise")
    sanity_check_outputs(tb)
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()


def sanity_check_outputs(tb: Table) -> None:
    cols = ["species_in_lpi", "species_not_in_lpi", "species_total"]
    assert tb[cols].notnull().all().all() and (tb[cols] >= 0).all().all(), "Missing or negative species counts."
    assert (tb["species_in_lpi"] + tb["species_not_in_lpi"] == tb["species_total"]).all(), (
        "Species included + not included does not equal the total."
    )

"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Entities published by the producer (World, the five IPBES regions, and the freshwater, marine and terrestrial systems).
# The producer already uses OWID-style names, so no harmonization is needed.
EXPECTED_ENTITIES = {
    "World",
    "Africa",
    "Asia and Pacific",
    "Europe and Central Asia",
    "Latin America and the Caribbean",
    "North America",
    "Freshwater",
    "Marine",
    "Terrestrial",
}
BASE_YEAR = 1970


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("living_planet_index")
    tb = ds_meadow.read("living_planet_index")

    #
    # Process data.
    #
    tb["country"] = tb["country"].astype(str)
    sanity_check_outputs(tb)
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def sanity_check_outputs(tb: Table) -> None:
    entities = set(tb["country"])
    assert entities == EXPECTED_ENTITIES, f"Unexpected entities: {entities ^ EXPECTED_ENTITIES}"
    cols = ["lpi_final", "ci_low", "ci_high"]
    assert tb[cols].notnull().all().all(), "Missing values in index or confidence intervals."
    assert (tb[cols] > 0).all().all(), "Index values must be positive."
    # Index is set to 1 in the base year, for every entity.
    base = tb[tb["year"] == BASE_YEAR]
    assert len(base) == len(EXPECTED_ENTITIES), f"Missing {BASE_YEAR} rows."
    assert (base[cols] == 1).all().all(), f"Index is not 1 in {BASE_YEAR}."
    # Central estimate lies within its confidence interval.
    assert (tb["ci_low"] <= tb["lpi_final"]).all() and (tb["lpi_final"] <= tb["ci_high"]).all(), (
        "Index outside its confidence interval."
    )
    # Every entity covers the same, contiguous range of years.
    years = tb.groupby("country")["year"].agg(["min", "max", "count"])
    assert (years["min"] == BASE_YEAR).all() and (years["count"] == years["max"] - years["min"] + 1).all(), (
        f"Non-contiguous year coverage:\n{years}"
    )

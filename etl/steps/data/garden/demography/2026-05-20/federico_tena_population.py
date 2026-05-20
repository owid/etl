"""Harmonize country names in the Federico–Tena V2 (1991 borders) population table."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("federico_tena_population")
    tb = ds_meadow.read("federico_tena_population")

    #
    # Process data.
    #
    # Harmonize country names against the OWID standard list. Unmapped entries are
    # written to federico_tena_population.countries.json on first run for review.
    tb = paths.regions.harmonize_names(
        tb=tb,
        country_col="country",
        countries_file=paths.country_mapping_path,
    )

    # We keep the source-side continent label and historical name as auxiliary
    # columns so downstream steps can inspect them, but they are not indicators.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


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
    tb = paths.regions.harmonize_names(tb, countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

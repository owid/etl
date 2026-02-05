"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("census_us_idb")

    # Read table from meadow dataset.
    tb = ds_meadow.read("census_us_idb")

    #
    # Process data.
    #

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Keep relevant countries
    # https://api.census.gov/data/timeseries/idb/5year/variables.html
    cols_index = ["country", "year"]
    cols = [
        "pop",
        "tfr",
        "e0",
        "nim",
    ] + cols_index
    # Improve table format.
    tb = tb[cols].format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

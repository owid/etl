"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read its data.
    tb = paths.load_snapshot("hawaii_ocean_time_series.csv").read(skiprows=8, sep="\t", na_values=[-999])

    #
    # Process data.
    #

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["date"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()

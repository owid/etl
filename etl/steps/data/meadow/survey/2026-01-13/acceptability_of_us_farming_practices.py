"""Load a snapshot and create a meadow dataset."""

import warnings

from etl.helpers import PathFinder

# Suppress POSIXct warnings from rdata library.
warnings.filterwarnings("ignore", message="Missing constructor for R class")

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("acceptability_of_us_farming_practices.rdata")

    # Load data from snapshot.
    tb = snap.read_rda()

    # Improve table format.
    tb = tb.format(["response_id"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset.

Import population data from Gapminder. Very little processing is done.

More details at https://www.gapminder.org/data/documentation/gd003/.
"""

from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("population.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("population.xlsx")

    # Load data from snapshot.
    tb = snap.read(
        sheet_name="data-for-countries-etc-by-year",
        usecols=["name", "time", "Population"],
    ).rename(columns={"name": "country", "time": "year", "Population": "population"})
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("population.end")

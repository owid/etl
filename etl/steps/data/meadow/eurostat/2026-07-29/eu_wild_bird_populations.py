"""Load a snapshot and create a meadow dataset."""

import gzip

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("eu_wild_bird_populations.gz")

    # Read compressed TSV file.
    with gzip.open(snap.path, "rt", encoding="utf-8") as f:
        tb = pr.read_csv(
            f, sep=r",|\t", engine="python", metadata=snap.to_table_metadata(), origin=snap.metadata.origin
        )

    #
    # Process data.
    #
    tb = tb.drop(columns=["freq", "statinfo", "geo"], errors="raise")

    tb = tb.rename(columns={"unit\TIME_PERIOD": "index_year", "comspec": "species"})

    tb["country"] = "European Union (27)"

    tb["index_year"] = tb["index_year"].replace({"I00": "2000", "I90": "1990"})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "species", "index_year"])
    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save changes in the new meadow dataset.
    ds_meadow.save()

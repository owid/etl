"""Load a snapshot and create a meadow dataset.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    tb = paths.load_snapshot("chick_culling_laws.csv").read()

    #
    # Process data.
    #
    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

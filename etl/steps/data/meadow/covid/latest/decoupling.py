"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_usa = paths.load_snapshot("decoupling_usa.csv")
    snap_spain = paths.load_snapshot("decoupling_spain.csv")
    snap_israel = paths.load_snapshot("decoupling_israel.csv")

    # Load data from snapshot.
    tb_usa = snap_usa.read()
    tb_spain = snap_spain.read()
    tb_israel = snap_israel.read()

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb_usa.format(["country", "year"]),
        tb_spain.format(["provincia_iso", "sexo", "grupo_edad", "fecha"]),
        tb_israel.format(["date"]),
    ]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap_usa.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

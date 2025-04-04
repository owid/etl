"""Script to create a snapshot of dataset 'Number of objects in space (United States Space Force, 2023)'.

Go to https://www.space-track.org and log in (create an account if you don't have one).
Go to Query Builder.
Use the following parameters: Class = gp, Order by = OBJECT_ID, Format = CSV
Leave the rest of the parameters as default.
Click on BUILD QUERY then RUN QUERY to download the CSV file (about 34 MB).

NOTE: The data seems to be updated regularly, so it's safe to assume that date_publised equals date_accessed.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"space/{SNAPSHOT_VERSION}/objects_in_space.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, filename=path_to_file)


if __name__ == "__main__":
    main()

"""Script to create a snapshot of dataset 'Countries influence the trade-off between crop yields and nitrogen pollution - Wuepper et al. (2020)'."""

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
    # Create a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/wuepper_et_al_2020.xlsx")
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

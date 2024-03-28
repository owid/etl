"""Script to create a snapshot of dataset 'Climate change has likely already affected global food production - Ray et al. (2019)'."""

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
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/ray_et_al_2019.xlsx")
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()

"""Script to create a snapshot of dataset 'Closing yield gaps through nutrient and water management - Mueller et al. (2012)'."""

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
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/mueller_et_al_2012.xls")
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()

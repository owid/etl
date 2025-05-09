"""Script to create a snapshot of dataset."""

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
    for file in ["number_of_wild_fish_killed_for_food_global", "number_of_wild_fish_killed_for_food_by_country"]:
        # Create a new snapshot.
        snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/{file}.pdf")

        # Download data and save snapshot.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()

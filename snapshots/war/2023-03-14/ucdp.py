"""Script to create a snapshot of dataset 'COW War Data, 1816 – 2007 (v4.0)'.

This dataset is built from four different datasets:

    - Non-state wars: The use of armed force between two organised armed groups.
    - Intra-state wars: A conflict between a government and a non-governmental party, with no interference from other countries.
    - Inter-state wars: A conflict between two or more governments.
    - Extra-state wars: An extra-systemic conflict is a conflict between a state and a non-state group outside its own territory. These conflicts are by definition territorial, since the government side is fighting to retain control of a territory outside the state system.
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
def main(upload: bool) -> None:
    snapshot_paths = [
        f"war/{SNAPSHOT_VERSION}/ucdp.one_sided.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp.non_state.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp.battle_related_conflict.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp.battle_related_dyadic.zip",
    ]
    for path in snapshot_paths:
        snap = Snapshot(path)
        snap.download_from_source()
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

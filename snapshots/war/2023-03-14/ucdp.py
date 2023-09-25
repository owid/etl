"""Script to create a snapshot of dataset 'COW War Data, 1816 – 2007 (v4.0)'.

This dataset is built from four different datasets:

    - One-sided Violence: An actor-year dataset with information of intentional attacks on civilians by governments and formally organized armed groups.
    - Non-State Conflict: A conflict-year dataset containing information on communal and organized armed conflict where none of the parties is the government of a state.
    - Battle-Related Deaths: A dyad-year dataset with information on the number of battle-related deaths in the conflicts from 1989-2021 that appear in the UCDP/PRIO Armed Conflict Dataset.

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
        f"war/{SNAPSHOT_VERSION}/ucdp_one_sided.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp_non_state.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp_battle_related_conflict.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp_battle_related_dyadic.zip",
    ]
    for path in snapshot_paths:
        snap = Snapshot(path)
        snap.download_from_source()
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

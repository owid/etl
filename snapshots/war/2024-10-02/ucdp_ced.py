"""Script to create a snapshot of dataset 'COW War Data, 1816 – 2007 (v4.0)'.

This dataset is built from four different datasets:

    - One-sided Violence: An actor-year dataset with information of intentional attacks on civilians by governments and formally organized armed groups.
    - Non-State Conflict: A conflict-year dataset containing information on communal and organized armed conflict where none of the parties is the government of a state.
    - Battle-Related Deaths: A dyad-year dataset with information on the number of battle-related deaths in the conflicts from 1989-2021 that appear in the UCDP/PRIO Armed Conflict Dataset.
    - Georeferenced Event Dataset: UCDP's most disaggregated dataset, covering individual events of organized violence (at village, single-day level).

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
    snapshot_path = f"war/{SNAPSHOT_VERSION}/ucdp_ced.csv"
    snap = Snapshot(snapshot_path)
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

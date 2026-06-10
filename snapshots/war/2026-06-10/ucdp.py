"""Script to create snapshots of the UCDP yearly release datasets.

This update is built from six different UCDP datasets:

    - One-sided Violence: An actor-year dataset with information of intentional attacks on civilians by governments and formally organized armed groups.
    - Non-State Conflict: A conflict-year dataset containing information on communal and organized armed conflict where none of the parties is the government of a state.
    - Battle-Related Deaths (conflict- and dyad-level): Information on the number of battle-related deaths in the conflicts that appear in the UCDP/PRIO Armed Conflict Dataset.
    - Georeferenced Event Dataset: UCDP's most disaggregated dataset, covering individual events of organized violence (at village, single-day level).
    - UCDP/PRIO Armed Conflict Dataset: A conflict-year dataset with information on armed conflict where at least one party is the government of a state.

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
def run(upload: bool) -> None:
    snapshot_paths = [
        f"war/{SNAPSHOT_VERSION}/ucdp_one_sided.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp_non_state.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp_battle_related_conflict.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp_battle_related_dyadic.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp_ged.zip",
        f"war/{SNAPSHOT_VERSION}/ucdp_prio_armed_conflict.zip",
    ]
    for path in snapshot_paths:
        snap = Snapshot(path)
        snap.download_from_source()
        snap.dvc_add(upload=upload)

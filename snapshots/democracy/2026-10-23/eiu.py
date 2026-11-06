"""Script to create a snapshot of the EIU Democracy Index.

EIU sent this spreadsheet to OWID directly; it is not a public download. It contains the overall score, rank and regime type of each country, for every edition since 2006. Pass its path to this script:

    etls democracy/<version>/eiu --path-to-file EIU_Democracy_Index.xlsx

NOTE: Checked against EIU's own reports, the history in this spreadsheet has errors (Ireland and Italy swapped in 2018-2021, Sweden and Switzerland in 2021, a 2008 column that repeats the 2011 values, scrambled ranks and regime labels). That's why the meadow step takes only the latest years from it, and keeps the older years from the snapshots transcribed from EIU's yearly reports. Re-check this at every update.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
        path_to_file: Path to local data file.
    """
    # Init Snapshot object
    snap = paths.init_snapshot()

    # Save snapshot from local file.
    snap.create_snapshot(filename=path_to_file, upload=upload)

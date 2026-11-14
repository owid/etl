"""Script to create a snapshot of the EIU Democracy Index.

EIU sent this spreadsheet to OWID directly; it is not a public download. It contains the overall score, rank and regime type of each country, for every edition since 2006. Pass its path to this script:

    etls democracy/<version>/eiu --path-to-file EIU_Democracy_Index.xlsx

NOTE: Check the whole history against EIU's yearly reports at every update. In the file EIU sent in November 2026 (a corrected version of an earlier one), the overall scores match the reports and the ranks follow the scores, but many regime labels contradict the scores, mostly in 2011-2016. That's why the garden step derives the regime type from the score.
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

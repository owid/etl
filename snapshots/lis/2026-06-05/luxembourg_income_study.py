"""Create the four LIS snapshots from the CSV files downloaded from the LIS FTP platform.

A single script handles all four snapshots: each LIS release ships the same set of CSVs, so
instead of one near-identical script per file we loop over them here.

To update:

1. Download the files from the LIS FTP platform: https://ftps.lisdatacenter.org/file
   (ask for credentials if you don't have them).
2. Copy them into this directory (snapshots/lis/<version>/). The downloaded files carry a
   timestamp suffix (e.g. ``absolute_poverty_2026_06_05.csv``) — leave it, this script strips
   it automatically by matching on the filename prefix.
3. Create all four snapshots with a single command:
       etls lis/<version>/luxembourg_income_study
   (pass ``--path-to-file <folder>`` to read the CSVs from somewhere other than this directory.)
4. Delete the leftover csv files:
       rm snapshots/lis/<version>/*.csv
"""

from pathlib import Path

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map each snapshot (the `.dvc` next to this script) to the prefix of the CSV downloaded from LIS.
# The downloaded files may carry a trailing date stamp (e.g. "incomes_2026_06_05.csv"), so we match
# on the prefix rather than the exact name.
FILES = {
    "lis_absolute_poverty.csv": "absolute_poverty",
    "lis_incomes.csv": "incomes",
    "lis_inequality.csv": "inequality",
    "lis_relative_poverty.csv": "relative_poverty",
}


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    """Create the LIS snapshots.

    Args:
        upload: Whether to upload the snapshots to S3.
        path_to_file: Folder containing the four input CSVs. Defaults to this snapshot directory.
    """
    folder = Path(path_to_file) if path_to_file else paths.snapshot_dir

    for snapshot_file, prefix in FILES.items():
        # Find the single CSV for this prefix, with or without a trailing date stamp.
        matches = sorted(folder.glob(f"{prefix}*.csv"))
        if len(matches) != 1:
            raise FileNotFoundError(
                f"Expected exactly one CSV matching '{prefix}*.csv' in {folder}, found {len(matches)}: "
                f"{[m.name for m in matches]}. Make sure only the freshly downloaded files are present."
            )

        snap = paths.init_snapshot(filename=snapshot_file)
        snap.create_snapshot(filename=matches[0], upload=upload)

"""
Script to create a snapshot of dataset.

STEPS TO EXTRACT DATA:

1. Go to https://www.cso.ie/en/census/censusthroughhistory/
2. Navigate to "What was the population in each census?"
3. Select the HTML table in Inspect mode, click on "Edit as HTML" and copy
4. Paste the HTML code in ChatGPT and ask
    Can you transform this html table into a csv? Get rid of the thousands separators in the data
5. Copy the output CSV data into a local file named population_ireland.csv in the same folder as this script.
6. Run
    python snapshots/demography/2025-12-10/population_ireland.py --path-to-file snapshots/demography/2025-12-10/population_ireland.csv

"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str = None) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
        path_to_file: Path to local data file.
    """
    # Init Snapshot object
    snap = paths.init_snapshot()

    # Save snapshot from local file.
    snap.create_snapshot(filename=path_to_file, upload=upload)

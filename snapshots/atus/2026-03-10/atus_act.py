"""Script to create a snapshot of dataset.
As direct download of the data files from the BLS website results in a 403 - forbidden error, the data file must be downloaded manually at http://www.bls.gov/tus/datafiles/atusact-0324.zip or https://www.bls.gov/tus/data/datafiles-0324.htm and uploaded here."""

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
"""To be able to download the dataset, you need to be registered at HFD.

Just go to their site: https://www.humanfertility.org/
Or directly try to login: https://www.humanfertility.org/Account/Login / register: https://www.humanfertility.org/Account/Auth

Only after that, you'll be allowed to download the dataset: https://www.humanfertility.org/File/Download/Files/zip/HFD.zip
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

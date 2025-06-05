"""Script to create a snapshot of dataset."""

from etl.snapshot import Snapshot

{% if cookiecutter.dataset_manual_import == True %}
def run(upload: bool = True, path_to_file: str = None) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
        path_to_file: Path to local data file.
    """
    # Create a new snapshot using the script's location.
    snap = Snapshot.from_script(__file__)

    # Save snapshot from local file.
    snap.create_snapshot(filename=path_to_file, upload=upload)

{% else %}
def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Create a new snapshot using the script's location.
    snap = Snapshot.from_script(__file__)

    # Save snapshot.
    snap.create_snapshot(upload=upload)

{% endif -%}

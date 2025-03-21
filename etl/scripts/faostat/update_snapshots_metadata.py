"""Update snapshots metadata, specifically, origin titles, origin descriptions and attributions.

NOTE: This script should be executed only after the garden steps are running properly, and custom titles and descriptions have been updated.

"""

import argparse

import pandas as pd
from structlog import get_logger

from etl.paths import SNAPSHOTS_DIR, STEP_DIR
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()


def run():
    # Get the latest version of faostat datasets.
    # NOTE: I assume that the latest published ETL version in garden contains all datasets.
    version = sorted((STEP_DIR / "data/garden/faostat/").glob("*/faostat_metadata.py"))[-1].parent.name

    # Load custom dataset titles and descriptions.
    df = pd.read_csv(STEP_DIR / f"data/garden/faostat/{version}/custom_datasets.csv")[
        ["dataset", "owid_dataset_title", "owid_dataset_description"]
    ]

    # List all snapshots.
    snapshot_names = [
        snapshot.name.replace(".dvc", "")
        for snapshot in (SNAPSHOTS_DIR / f"faostat/{version}").glob("*.dvc")
        if not snapshot.name.startswith("faostat_metadata")
    ]
    for snapshot_name in snapshot_names:
        # Load snapshot.
        snapshot = Snapshot(f"faostat/{version}/{snapshot_name}")

        # Load custom title and descriptions for the corresponding dataset.
        dataset_name = snapshot_name.split(".")[0]
        title = df[df["dataset"] == dataset_name]["owid_dataset_title"].item()
        description = df[df["dataset"] == dataset_name]["owid_dataset_description"].item()

        # Add a custom attribution to each origin.
        year_published = snapshot.metadata.origin.date_published.split("-")[0]  # type: ignore
        # Currently, FBSH and FBS had their latest update by FAOSTAT in different years.
        # This causes that the two origins are cited in all charts (with different years).
        # To avoid this, we add attribution of FBS to FBSH.
        if dataset_name == "faostat_fbsh":
            snapshot_fbs = Snapshot(f"faostat/{version}/faostat_fbs.zip")
            year_published = snapshot_fbs.metadata.origin.date_published.split("-")[0]  # type: ignore
        attribution = f"{snapshot.metadata.origin.producer} ({year_published})"  # type: ignore

        # Update metadata fields of the current snapshot.
        snapshot.metadata.origin.title = title  # type: ignore
        snapshot.metadata.origin.description = description  # type: ignore
        snapshot.metadata.origin.attribution = attribution  # type: ignore

        # Rewrite metadata to dvc file.
        ################################################################################################################
        # TODO: Ideally, it should suffice to do snapshot.metadata.save(), which would write the outs part of the snapshot as well, but that fails. So we'll add the outs part "manually" here.
        new_yaml = snapshot.metadata.to_yaml()
        outs = snapshot.metadata.outs[0]
        new_yaml += f"\nouts:\n  - md5: {outs['md5']}\n    size: {outs['size']}\n    path: {outs['path']}"
        snapshot.metadata_path.write_text(new_yaml)
        ################################################################################################################


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description=__doc__)
    args = argument_parser.parse_args()
    run()

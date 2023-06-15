"""Load a snapshot and create a meadow dataset."""

from typing import cast

import shared
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("papers_with_code_imagenet_top1.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_top1 = cast(Snapshot, paths.load_dependency("papers_with_code_imagenet_top1.html"))
    snap_top5 = cast(Snapshot, paths.load_dependency("papers_with_code_imagenet_top5.html"))

    # Load data from snapshot.
    # Read the HTML file
    with open(snap_top1.path, "r") as file:
        html_content = file.read()
    df_top1 = shared.extract_data_papers_with_code(html_content)

    with open(snap_top5.path, "r") as file:
        html_content = file.read()
    df_top5 = shared.extract_data_papers_with_code(html_content)

    #
    # Process data.
    #

    # Create a new table and ensure all columns are snake-case.
    tb_top1 = Table(df_top1, short_name="papers_with_code_imagenet_top1", underscore=True)
    tb_top5 = Table(df_top5, short_name="papers_with_code_imagenet_top5", underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb_top1, tb_top5], default_metadata=snap_top1.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("papers_with_code_imagenet_top1.end")

"""Load a snapshot and create a meadow dataset."""
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("sdg.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("sdg")

    # Load data from snapshot.
    tb = snap.read()

    tb = tb.drop(
        columns=[
            "indicator_id",
            "age_group_id",
            "sex_id",
            "scenario",
            "indicator_short",
            "indicator_description",
        ]
    )

    tb = tb.rename(columns={"location_name": "country", "year_id": "year"})
    #
    # Process data.
    #
    tb = tb.format(
        ["country", "location_id", "year", "indicator_name", "age_group_name", "sex_label", "scenario_label"]
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("sdg.end")

"""Load a snapshot and create a meadow dataset."""


import shared
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("yougov_job_automation.start")
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("yougov_job_automation.xlsx")

    #
    # Process data.
    #
    tb_all_sheets = shared.process_data(snap)

    tb_all_sheets = tb_all_sheets.underscore()
    tb_all_sheets = tb_all_sheets.set_index(
        [
            "how_worried__if_it_all__are_you_that_your_type_of_work_could_be_automated_within_your_lifetime",
            "date",
            "group",
        ],
        verify_integrity=True,
    ).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_all_sheets], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("yougov_job_automation.end")

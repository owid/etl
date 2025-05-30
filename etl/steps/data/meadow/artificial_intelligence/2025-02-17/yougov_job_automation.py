"""Load a snapshot and create a meadow dataset."""

import shared

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("yougov_job_automation.xlsx")

    #
    # Process data.
    #
    tb_all_sheets = shared.process_data(snap)

    tb_all_sheets = tb_all_sheets.format(
        [
            "how_worried__if_it_all__are_you_that_your_type_of_work_could_be_automated_within_your_lifetime",
            "date",
            "group",
        ]
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb_all_sheets], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()

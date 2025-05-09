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
    snap = paths.load_snapshot("yougov_robots.xlsx")

    #
    # Process data.
    #
    tb_all_sheets = shared.process_data(snap)

    tb_all_sheets = tb_all_sheets.format(
        ["which_one__if_any__of_the_following_statements_do_you_most_agree_with", "date", "group"]
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

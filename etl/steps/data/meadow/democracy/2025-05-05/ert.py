"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ert.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Keep relevant columns
    tb = tb[
        [
            "country_name",
            "year",
            "reg_type",
            "dem_ep",
            "aut_ep",
            "dem_ep_outcome",
            "dem_ep_end_year",
            "aut_ep_outcome",
            "aut_ep_end_year",
        ]
    ]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country_name", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

TREATIES = {
    "geneva_protocol": "Geneva Protocol",
    "partial_test_ban": "Partial Test Ban Treaty",
    "comprehensive_test_ban": "Comprehensive Nuclear-Test-Ban Treaty",
    "non_proliferation": "Nuclear Non-Proliferation Treaty",
    "prohibition": "Treaty on the Prohibition of Nuclear Weapons",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    data = []
    for treaty in TREATIES:
        # Retrieve snapshot and read its data.
        snap = paths.load_snapshot(f"{treaty}.csv")
        tb = snap.read().assign(**{"treaty": TREATIES[treaty]})
        data.append(tb)

    #
    # Process data.
    #
    # Concatenate all tables.
    tb = pr.concat(data, ignore_index=True, short_name=paths.short_name)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = (
        tb.underscore()
        .set_index(["treaty", "date", "state", "action", "depositary"], verify_integrity=True)
        .sort_index()
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()

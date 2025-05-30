"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

TREATIES = {
    "geneva_protocol": "Geneva Protocol",
    "partial_test_ban": "Partial Test Ban Treaty",
    "comprehensive_test_ban": "Comprehensive Nuclear-Test-Ban Treaty",
    "non_proliferation": "Nuclear Non-Proliferation Treaty",
    "prohibition": "Treaty on the Prohibition of Nuclear Weapons",
}


def run() -> None:
    #
    # Load inputs.
    #
    data = []
    for treaty_short_name, treaty_title in TREATIES.items():
        # Retrieve snapshot and read its data.
        snap = paths.load_snapshot(f"nuclear_weapons_treaties__{treaty_short_name}.csv")
        tb = snap.read(safe_types=False).assign(**{"treaty": treaty_title})
        data.append(tb)

    #
    # Process data.
    #
    # Concatenate all tables.
    tb = pr.concat(data, ignore_index=True, short_name=paths.short_name)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["treaty", "date", "state", "action", "depositary"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()

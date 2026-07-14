"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("yearly_burned_area.csv")
    tb = snap.read()

    #
    # Process data.
    #
    columns_to_keep = [
        "country",
        "year",
        "forest",
        "savannas",
        "shrublands_grasslands",
        "croplands",
        "other",
    ]
    tb = tb[columns_to_keep]
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()

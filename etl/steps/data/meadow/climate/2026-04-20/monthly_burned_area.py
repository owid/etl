"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("monthly_burned_area.csv")
    tb = pr.read_csv(snap.path, metadata=snap.to_table_metadata(), origin=snap.m.origin)

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
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()

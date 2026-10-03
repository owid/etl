"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("testing_coverage.zip")

    # Load data from snapshot.
    regions = [
        "African Region",
        "Region of the Americas",
        "South-East Asia Region",
        "European Region",
        "Eastern Mediterranean Region",
        "Western Pacific Region",
        "All",
    ]
    tables = []
    for region in regions:
        tb = snap.read_in_archive(
            filename=f"who_glass_testing_coverage/Testing coverage by infectious syndrome_{region}.csv", skiprows=4
        )
        tb["country"] = region
        tables.append(tb)

    tb = pr.concat(tables)
    tb = tb.rename(columns={"Year": "year"})
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "specimen"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

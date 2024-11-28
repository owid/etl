"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEARS = range(1990, 2022)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("microbe.zip")
    tables = []
    for year in YEARS:
        # Load data from snapshot.
        tb = snap.read_in_archive(filename=f"neonatal/pathogen_{year}.csv")
        tables.append(tb)
    tb = pr.concat(tables)
    #
    # Process data.
    tb = tb.rename(columns={"Location": "country", "Year": "year"})
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "pathogen"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

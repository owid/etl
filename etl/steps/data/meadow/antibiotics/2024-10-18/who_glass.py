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
    snap = paths.load_snapshot("who_glass.zip")

    tables = []
    # Load data from snapshot.
    for syndrome in ["URINE", "BLOOD", "STOOL", "UROGENITAL"]:
        for year in range(2016, 2023):
            tb = snap.read_in_archive(
                filename=f"{syndrome}_{year}.csv",
                skiprows=4,
            )
            tb["syndrome"] = syndrome
            tb["year"] = year
            tables.append(tb)

    tb = pr.concat(tables)
    # Process data.
    tb = tb.rename(columns={"CountryTerritoryArea": "country"})
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "syndrome"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

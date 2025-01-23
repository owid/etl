"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("iucn_number_each_status.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    exclude = ["Name"]
    cols = tb.columns.difference(exclude)
    tb[cols] = tb[cols].replace(",", "", regex=True)
    tb[cols] = tb[cols].astype("Int64")

    # Add the publication year as a column
    tb["year"] = snap.metadata.origin.date_published[:4]
    #
    tb = tb.rename(columns={"Name": "country"})
    # Change the taxonomic groups to only have the first letter capitalized
    tb["country"] = tb["country"].str.capitalize()

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("endemic_vertebrates.csv")
    # Load data from snapshot.
    tb = snap.read()
    tb["year"] = 2025
    tb = tb.rename(columns={"region_or_country": "country"})
    # There are some exact duplicate rows in the source file, so we drop these here. Probably because the data is scraped from a pdf.
    tb = tb.drop_duplicates()
    tb = tb.format(["country", "year"])

    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

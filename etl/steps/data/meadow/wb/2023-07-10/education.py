from etl.helpers import PathFinder, create_dataset

# Create a PathFinder instance for the current file
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    snap = paths.load_snapshot("education.csv")

    # Load data from snapshot.
    tb = snap.read_csv(low_memory=False)

    # Rename indicator code and name columns
    tb = tb.rename(columns={"Series": "indicator_name", "wb_seriescode": "indicator_code"})

    tb = tb.underscore().set_index(["country", "year", "indicator_code"], verify_integrity=True)

    # Use metadata from the first snapshot, then edit the descriptions in the garden step
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)

    # Save the dataset
    ds_meadow.save()

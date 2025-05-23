from etl.helpers import PathFinder, create_dataset

# Create a PathFinder instance for the current file
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    snap = paths.load_snapshot("gender_statistics.feather")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)
    tb = tb.format(["country", "year", "wb_seriescode"])

    # Drop indicator_name column series column as it should be roughgly the same as indicator_name column (long definition of the indicator)
    tb = tb.drop(columns=["series"])
    # Use metadata from the first snapshot, then edit the descriptions in the garden step
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)

    # Save the dataset
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("impairments.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = clean_data(tb)
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "metric", "cause", "impairment", "age", "sex"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def clean_data(tb: Table) -> Table:
    tb = tb.rename(
        columns={
            "location_name": "country",
            "val": "value",
            "rei_name": "impairment",
            "measure_name": "measure",
            "sex_name": "sex",
            "age_name": "age",
            "cause_name": "cause",
            "metric_name": "metric",
        },
        errors="ignore",
    )
    tb = tb.drop(
        columns=[
            "measure_id",
            "location_id",
            "sex_id",
            "age_id",
            "cause_id",
            "metric_id",
            "rei_id",
            "upper",
            "lower",
            "measure",
        ],
        errors="ignore",
    )
    tb = tb.astype(
        {
            "country": "category",
            "value": "float32",
            "impairment": "category",
            "sex": "category",
            "age": "category",
            "cause": "category",
            "metric": "category",
            "year": "int",
        }
    )
    return tb

"""Load a snapshot and create a meadow dataset."""

import os

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("github_stats_vax_reporting.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb["country"] = tb["country"].apply(extract_filename_without_extension)
    tb["date"] = tb["date_first_reported"]
    tb = tb[["country", "date", "date_first_reported", "date_first_value"]]

    # Dtypes
    tb = tb.astype(
        {
            "country": "string",
            "date": "datetime64[ns]",
            "date_first_reported": "datetime64[ns]",
            "date_first_value": "datetime64[ns]",
        }
    )

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "date"], short_name="vaccinations")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def extract_filename_without_extension(file_path):
    # Get the base name (filename with extension)
    base_name = os.path.basename(file_path)
    # Split the base name into name and extension
    name, _ = os.path.splitext(base_name)
    return name

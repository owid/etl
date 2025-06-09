"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define file path
FILE_PATH = "RR_WLD_2025_346/Reproducibility Package/Reproducibility/data/02_processed/harm_npl_results.dta"


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("global_poverty_revisited_2021_ppps.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive(FILE_PATH)

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country_code", "year", "reporting_level", "welfare_type"], short_name=paths.short_name)]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

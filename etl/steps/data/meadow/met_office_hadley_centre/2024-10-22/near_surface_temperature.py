"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select and how to name them.
COLUMNS = {
    # Additional column.
    "region": "region",
    # Original column names and new names.
    "Time": "year",
    "Anomaly (deg C)": "temperature_anomaly",
    "Lower confidence limit (2.5%)": "lower_limit",
    "Upper confidence limit (97.5%)": "upper_limit",
}

# Names of snapshot files.
REGION_FILE_NAMES = {
    "Global": "near_surface_temperature_global.csv",
    "Northern hemisphere": "near_surface_temperature_northern_hemisphere.csv",
    "Southern hemisphere": "near_surface_temperature_southern_hemisphere.csv",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snapshots = {region: paths.load_snapshot(file_name) for region, file_name in REGION_FILE_NAMES.items()}

    # Load data from snapshots.
    tb = pr.concat(
        [snapshot.read().assign(**{"region": region}) for region, snapshot in snapshots.items()],
        ignore_index=True,
        short_name=paths.short_name,
    )

    #
    # Process data.
    #
    # Select and rename required columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["region", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()

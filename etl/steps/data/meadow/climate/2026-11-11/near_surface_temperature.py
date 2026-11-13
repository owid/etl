"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

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


def run() -> None:
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
    tb = tb.format(["region", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()

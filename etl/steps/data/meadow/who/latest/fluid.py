"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("fluid.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("fluid.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)
    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    # Dropping out these columns as they are awkward types and we don't need to use them
    tb = tb.drop(columns=["comments", "geospread_comments"])
    tb = tb.rename(columns={"country_area_territory": "country"})

    # Convert object columns that should be numeric to numeric
    # Using errors='coerce' converts invalid values to NaN rather than failing.
    numeric_cols = [
        "ili_case",
        "ili_outpatients",
        "ili_pop_cov",
        "sari_case",
        "sari_inpatients",
        "sari_pop_cov",
        "sari_deaths",
        "ari_case",
        "ari_outpatients",
        "ari_pop_cov",
        "pneu_case",
        "pneu_inpatients",
        "pneu_pop_cov",
    ]
    for col in numeric_cols:
        if col in tb.columns:
            # Track non-null values before conversion
            before_non_null = tb[col].notna().sum()
            original_values = tb[col].copy()

            # Convert to numeric, coercing errors to NaN
            tb[col] = pd.to_numeric(tb[col], errors="coerce")

            # Check if any values were coerced to NaN (data quality issues)
            after_non_null = tb[col].notna().sum()
            coerced_count = before_non_null - after_non_null

            if coerced_count > 0:
                # Find the problematic rows
                coerced_mask = original_values.notna() & tb[col].isna()
                problematic_rows = tb[coerced_mask]

                log.warning(
                    f"Data quality issue in column '{col}': {coerced_count} non-numeric values coerced to NaN",
                    col=col,
                    coerced_count=coerced_count,
                    sample_values=original_values[coerced_mask].head(5).tolist(),
                    sample_countries=problematic_rows["country"].head(5).tolist() if "country" in tb.columns else None,
                    sample_dates=problematic_rows["iso_weekstartdate"].head(5).tolist()
                    if "iso_weekstartdate" in tb.columns
                    else None,
                )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("fluid.end")

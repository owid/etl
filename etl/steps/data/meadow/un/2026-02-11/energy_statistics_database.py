"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Expected number of rows (approximate, to catch major data issues).
EXPECTED_MIN_ROWS = 1_500_000


def sanity_check_inputs(tb: Table) -> None:
    """Check raw data before processing."""
    assert len(tb) > EXPECTED_MIN_ROWS, f"Expected at least {EXPECTED_MIN_ROWS} rows, got {len(tb)}"
    assert tb["TRANSACTION"].notna().all(), "Unexpected null TRANSACTION values (check NA parsing)"
    assert tb["COMMODITY"].notna().all(), "Unexpected null COMMODITY values"
    assert tb["REF_AREA"].notna().all(), "Unexpected null REF_AREA values"
    assert tb["OBS_VALUE"].notna().all(), "Unexpected null OBS_VALUE values"


def sanity_check_outputs(tb: Table) -> None:
    """Check processed data after codelist mapping."""
    assert tb["country"].notna().all(), f"Unmapped REF_AREA codes: {tb[tb['country'].isna()]['REF_AREA'].unique()}"
    assert (
        tb["commodity"].notna().all()
    ), f"Unmapped COMMODITY codes: {tb[tb['commodity'].isna()]['COMMODITY'].unique()}"
    assert (
        tb["transaction"].notna().all()
    ), f"Unmapped TRANSACTION codes: {tb[tb['transaction'].isna()]['TRANSACTION'].unique()}"
    assert tb["unit"].notna().all(), f"Unmapped UNIT_MEASURE codes: {tb[tb['unit'].isna()]['UNIT_MEASURE'].unique()}"


def read_snapshot(snap):
    """Read main data and codelists from the snapshot zip archive."""
    with snap.extracted() as archive:
        # NOTE: keep_default_na=False is critical because TRANSACTION code "NA" (Final consumption)
        # would otherwise be parsed as NaN by pandas.
        tb = pr.read_csv(
            archive.path / "energy_statistics_database.csv",
            dtype={"REF_AREA": str, "COMMODITY": str, "TRANSACTION": str},
            keep_default_na=False,
            na_values=[""],
            metadata=snap.to_table_metadata(),
            origin=snap.metadata.origin,
        )
        # NOTE: keep_default_na=False is also needed for codelists (TRANSACTION code "NA").
        code_area = pd.read_csv(archive.path / "codelist_REF_AREA.csv", dtype=str, keep_default_na=False)
        code_commodity = pd.read_csv(archive.path / "codelist_COMMODITY.csv", dtype=str, keep_default_na=False)
        code_transaction = pd.read_csv(archive.path / "codelist_TRANSACTION.csv", dtype=str, keep_default_na=False)
        code_unit = pd.read_csv(archive.path / "codelist_UNIT_MEASURE.csv", dtype=str, keep_default_na=False)

    return tb, code_area, code_commodity, code_transaction, code_unit


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("energy_statistics_database.zip")
    tb, code_area, code_commodity, code_transaction, code_unit = read_snapshot(snap)

    # Sanity check inputs.
    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Map coded dimensions to human-readable names using codelists.
    tb["country"] = tb["REF_AREA"].map(code_area.set_index("code")["name"])
    tb["commodity"] = tb["COMMODITY"].map(code_commodity.set_index("code")["name"])
    tb["transaction"] = tb["TRANSACTION"].map(code_transaction.set_index("code")["name"])
    tb["unit"] = tb["UNIT_MEASURE"].map(code_unit.set_index("code")["name"])

    # Sanity check outputs.
    sanity_check_outputs(tb)

    # Rename year and value columns.
    tb = tb.rename(columns={"TIME_PERIOD": "year", "OBS_VALUE": "value"})

    # Keep only the resolved columns and the value.
    tb = tb[["country", "year", "commodity", "transaction", "unit", "value"]].copy()

    # Set index and sort.
    tb = tb.format(["country", "year", "commodity", "transaction", "unit"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

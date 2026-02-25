"""FAOSTAT meadow step for faostat_fbs dataset.
########################################################################################################################
TEMPORARY STEP WHILE FAOSTAT FBS FIXES THEIR MISSING DATA!

As they mention in their update history (from https://www.fao.org/faostat/en/#data/FBS, click on "Update history" on the bottom right):
"Data for Benin, Burundi, Central African Republic, Chad, Dominica, Japan, Mali, Somalia, South Sudan, Sudan and Togo are not available due to an ongoing review"
These countries simply don't have any data!
For now, fetch data for those countries from the previous snapshot.

Once that's fixed, replace this step by the one-line import of shared:
from .shared import run  # noqa:F401
########################################################################################################################
"""

import os
import tempfile
import zipfile
from pathlib import Path

import owid.catalog.processing as pr
import structlog
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialise log.
log = structlog.get_logger()

# Define path to current folder, namespace and version of all datasets in this folder.
CURRENT_DIR = Path(__file__).parent
NAMESPACE = CURRENT_DIR.parent.name
VERSION = CURRENT_DIR.name


def load_data(snapshot: Snapshot) -> Table:
    """Load snapshot data (as a table) for current dataset.

    Parameters
    ----------
    local_path : Path or str
        Path to local snapshot file.

    Returns
    -------
    data : Table
        Snapshot data.

    """
    # Unzip data into a temporary folder.
    with tempfile.TemporaryDirectory() as temp_dir:
        z = zipfile.ZipFile(snapshot.path)
        z.extractall(temp_dir)
        (filename,) = list(filter(lambda x: "(Normalized)" in x, os.listdir(temp_dir)))

        # Load data from main file.
        try:
            data = pr.read_csv(
                os.path.join(temp_dir, filename),
                encoding="utf-8",
                low_memory=False,
                origin=snapshot.metadata.origin,
                metadata=snapshot.to_table_metadata(),
            )
        except UnicodeDecodeError:
            data = pr.read_csv(
                os.path.join(temp_dir, filename),
                encoding="windows-1252",
                low_memory=False,
                origin=snapshot.metadata.origin,
                metadata=snapshot.to_table_metadata(),
            )

    return data


def run_sanity_checks(tb: Table) -> None:
    """Run basic sanity checks on loaded data (raise assertion errors if any check fails).

    Parameters
    ----------
    tb : Table
        Data to be checked.

    """
    tb = tb.copy()

    # Check that column "Year Code" is identical to "Year", and can therefore be dropped.
    error = "Column 'Year Code' does not coincide with column 'Year'."
    if "Year" not in tb.columns:
        pass
        # Column 'Year' is not in data (this happens at least in faostat_wcad, which requires further processing).
    elif tb["Year"].dtype == int:
        # In most cases, columns "Year Code" and "Year" are simply the year.
        assert (tb["Year Code"] == tb["Year"]).all(), error
    else:
        # Sometimes (e.g. for dataset fs) there are year ranges (e.g. with "Year Code" 20002002 and "Year" "2000-2002").
        assert (tb["Year Code"] == tb["Year"].str.replace("-", "").astype(int)).all(), error

    # Check that there is only one element-unit for each element code.
    error = "Multiple element-unit for the same element code."
    assert (tb.groupby(["Element", "Unit"])["Element Code"].nunique() == 1).all(), error


def prepare_output_data(tb: Table) -> Table:
    """Prepare data before saving it to meadow.

    Parameters
    ----------
    tb : Table
        Data.

    Returns
    -------
    tb : Table
        Data ready to be stored as a table in meadow.

    """
    tb = tb.copy()

    # Select columns to keep.
    # Note:
    # * Ignore column "Year Code" (which is almost identical to "Year", and does not add information).
    # * Ignore column "Note" (which is included only in faostat_fa, faostat_fs, faostat_sdgb and faostat_wcad datasets).
    #   This column may contain double-quoted text within double-quoted text, which becomes impossible to parse.
    #   E.g. faostat_wcad line 105.
    # * Add "Recipient Country Code" and "Recipient Code", which are the names for "Area Code" and "Area", respectively,
    #   for dataset faostat_fa.
    columns_to_keep = [
        "Area Code",
        "Area",
        "Year",
        "Item Code",
        "Item",
        "Element Code",
        "Element",
        "Unit",
        "Value",
        "Flag",
        "Recipient Country Code",
        "Recipient Country",
    ]
    # Select only columns that are found in the table.
    columns_to_keep = list(set(columns_to_keep) & set(tb.columns))
    tb = tb.loc[:, columns_to_keep]

    # Set index columns depending on what columns are available in the table.
    # Note: "Recipient Country Code" appears only in faostat_fa, and seems to replace "Area Code".
    index_columns = list({"Area Code", "Recipient Country Code", "Year", "Item Code", "Element Code"} & set(tb.columns))
    if tb.duplicated(subset=index_columns).any():
        log.warning("Index has duplicated keys.")

    # Ensure all columns are snake-case, and set an index.
    tb = tb.set_index(index_columns).underscore()

    return tb


def run() -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = "faostat_fbs"

    # Load snapshot.
    snapshot = paths.load_snapshot(version=VERSION)
    tb_snapshot = load_data(snapshot)

    # Load old snapshot.
    snapshot_old = paths.load_snapshot(version="2025-03-17")
    tb_snapshot_old = load_data(snapshot_old)

    #
    # Process data.
    #
    # Run sanity checks.
    run_sanity_checks(tb=tb_snapshot)
    run_sanity_checks(tb=tb_snapshot_old)

    # Prepare output meadow table.
    tb = prepare_output_data(tb=tb_snapshot)
    tb_old = prepare_output_data(tb=tb_snapshot_old)

    # Check that column "value" has an origin (other columns are not as important and may not have origins).
    assert len(tb["value"].metadata.origins) == 1, f"Column 'value' of {dataset_short_name} must have one origin."
    assert len(tb_old["value"].metadata.origins) == 1, f"Column 'value' of {dataset_short_name} must have one origin."

    # Remove the origins from the old table.
    for column in tb_old.columns:
        tb_old[column].metadata.origins = []

    # Check that new snapshot misses data for the expected list of countries.
    MISSING_COUNTRIES = [
        "Benin",
        "Burundi",
        "Central African Republic",
        "Chad",
        "Dominica",
        "Japan",
        "Mali",
        "Somalia",
        "South Sudan",
        "Sudan",
        "Togo",
    ]
    error = "Expected missing countries are present in the new FBS snapshot. Check that all countries are informed and, if so, remove this temporary step (as explained in docstring)."
    assert set(MISSING_COUNTRIES) & set(tb["area"]) == set(), error
    error = "Missing countries are not informed in the old snapshot (this should not happen)."
    assert set(MISSING_COUNTRIES) < set(tb_old["area"]), error
    # Append data for missing countries to the new table.
    tb = pr.concat([tb.reset_index(), tb_old[tb_old["area"].isin(MISSING_COUNTRIES)].reset_index()], ignore_index=True)

    # Improve table format.
    tb = tb.format(keys=["item_code", "year", "element_code", "area_code"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()

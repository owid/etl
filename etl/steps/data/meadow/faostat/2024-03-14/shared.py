"""Shared definitions in FAOSTAT meadow steps.

Some basic processing is required to create tables from the raw data.
For example, column "Note" (present in some datasets) is skipped to avoid parsing errors.
Other minor changes can be found in the code.

"""

import os
import tempfile
import zipfile
from pathlib import Path

import owid.catalog.processing as pr
import structlog
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

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
        data = pr.read_csv(
            os.path.join(temp_dir, filename),
            encoding="latin-1",
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
    tb = tb[columns_to_keep]

    # Set index columns depending on what columns are available in the table.
    # Note: "Recipient Country Code" appears only in faostat_fa, and seems to replace "Area Code".
    index_columns = list({"Area Code", "Recipient Country Code", "Year", "Item Code", "Element Code"} & set(tb.columns))
    if tb.duplicated(subset=index_columns).any():
        log.warning("Index has duplicated keys.")

    # Ensure all columns are snake-case, and set an index.
    tb = tb.set_index(index_columns).underscore()

    return tb


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = Path(dest_dir).name

    # Define path to current step file.
    current_step_file = (CURRENT_DIR / dataset_short_name).with_suffix(".py")

    # Get paths and naming conventions for current data step.
    paths = PathFinder(current_step_file.as_posix())

    # Load snapshot.
    snapshot = paths.load_snapshot()
    tb_snapshot = load_data(snapshot)

    #
    # Process data.
    #
    # Run sanity checks.
    run_sanity_checks(tb=tb_snapshot)

    # Prepare output meadow table.
    tb = prepare_output_data(tb=tb_snapshot)

    # Check that column "value" has an origin (other columns are not as important and may not have origins).
    assert len(tb["value"].metadata.origins) == 1, f"Column 'value' of {dataset_short_name} must have one origin."

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    # NOTE: Do not check if all variables have metadata. We asserted above that "value" has an origin.
    ds_meadow = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=False)
    ds_meadow.save()

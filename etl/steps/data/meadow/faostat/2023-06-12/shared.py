"""Shared definitions in FAOSTAT meadow steps.

Some basic processing is required to create tables from the raw data.
For example, column "Note" (present in some datasets) is skipped to avoid parsing errors.
Other minor changes can be found in the code.

"""

import os
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import structlog
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Initialise log.
log = structlog.get_logger()

# Define path to current folder, namespace and version of all datasets in this folder.
CURRENT_DIR = Path(__file__).parent
NAMESPACE = CURRENT_DIR.parent.name
VERSION = CURRENT_DIR.name


def load_data(local_path: Path) -> pd.DataFrame:
    """Load snapshot data (as a dataframe) for current dataset.

    Parameters
    ----------
    local_path : Path or str
        Path to local snapshot file.

    Returns
    -------
    data : pd.DataFrame
        Snapshot data.

    """
    # Unzip data into a temporary folder.
    with tempfile.TemporaryDirectory() as temp_dir:
        z = zipfile.ZipFile(local_path)
        z.extractall(temp_dir)
        (filename,) = list(filter(lambda x: "(Normalized)" in x, os.listdir(temp_dir)))

        # Load data from main file.
        try:
            data = pd.read_csv(os.path.join(temp_dir, filename), encoding="latin-1")
        except pd.errors.ParserError:
            # Some files are impossible to parse (e.g. faostat_wcad) because column "Note" is poorly formatted.
            # Instead of skipping problematic rows, load the file skipping that problematic column.
            columns = pd.read_csv(
                os.path.join(temp_dir, filename), encoding="latin-1", on_bad_lines="skip", nrows=0
            ).columns
            columns = columns.drop("Note")
            data = pd.read_csv(os.path.join(temp_dir, filename), encoding="latin-1", usecols=columns)

    return data


def run_sanity_checks(data: pd.DataFrame) -> None:
    """Run basic sanity checks on loaded data (raise assertion errors if any check fails).

    Parameters
    ----------
    data : pd.DataFrame
        Data to be checked.

    """
    df = data.copy()

    # Check that column "Year Code" is identical to "Year", and can therefore be dropped.
    error = "Column 'Year Code' does not coincide with column 'Year'."
    if "Year" not in data.columns:
        pass
        # Column 'Year' is not in data (this happens at least in faostat_wcad, which requires further processing).
    elif df["Year"].dtype == int:
        # In most cases, columns "Year Code" and "Year" are simply the year.
        assert (df["Year Code"] == df["Year"]).all(), error
    else:
        # Sometimes (e.g. for dataset fs) there are year ranges (e.g. with "Year Code" 20002002 and "Year" "2000-2002").
        assert (df["Year Code"] == df["Year"].str.replace("-", "").astype(int)).all(), error

    # Check that there is only one element-unit for each element code.
    error = "Multiple element-unit for the same element code."
    assert (df.groupby(["Element", "Unit"])["Element Code"].nunique() == 1).all(), error


def prepare_output_data(data: pd.DataFrame) -> pd.DataFrame:
    """Prepare data before saving it to meadow.

    Parameters
    ----------
    data : pd.DataFrame
        Data.

    Returns
    -------
    df : pd.DataFrame
        Data ready to be stored as a table in meadow.

    """
    df = data.copy()

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
        # Additional columns for faostat_wcad.
        "WCA Round",
        "Census Year",
    ]
    # Select only columns that are found in the dataframe.
    columns_to_keep = list(set(columns_to_keep) & set(df.columns))
    df = df[columns_to_keep]

    # Set index columns depending on what columns are available in the dataframe.
    # Note: "Recipient Country Code" appears only in faostat_fa, and seems to replace "Area Code".
    # Note: "WCA Round" and "Census Year" appear only in faostat_wcad.
    index_columns = list(
        {"Area Code", "Recipient Country Code", "Year", "Item Code", "Element Code", "WCA Round", "Census Year"}
        & set(df.columns)
    )
    if df.duplicated(subset=index_columns).any():
        log.warning("Index has duplicated keys.")
    df = df.set_index(index_columns)

    return df


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
    snapshot = paths.load_dependency(short_name=dataset_short_name + ".zip", channel="snapshot")
    df_snapshot = load_data(snapshot.path)

    #
    # Process data.
    #
    # Run sanity checks.
    run_sanity_checks(data=df_snapshot)

    # Prepare output meadow table.
    tb_meadow = Table(prepare_output_data(data=df_snapshot), short_name=dataset_short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir=dest_dir, tables=[tb_meadow], default_metadata=snapshot.metadata)
    ds_meadow.save()

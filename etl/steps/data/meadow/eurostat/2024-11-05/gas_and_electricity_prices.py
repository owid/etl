"""Load a snapshot and create a meadow dataset."""

import zipfile

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gas_and_electricity_prices.zip")

    # Create a list to store each table.
    tables = []
    # Open the ZIP file and read each TSV file.
    with zipfile.ZipFile(snap.path, "r") as zip_file:
        for file_name in zip_file.namelist():
            # Read each TSV file into a table.
            with zip_file.open(file_name) as file:
                dataset_code = file_name.split(".")[0]
                # Each data file starts with comma-separated index columns, followed by tab-separated time data.
                # Example:
                # freq,product,nrg_cons,unit,tax,currency,geo\TIME_PERIOD   2007    2008...
                # And for some datasets, there is annual data, and for others bi-annual data, e.g. 2007-S1 2007-S2 2008-S1...
                # First, load this file as a table.
                _tb = pr.read_csv(
                    file, sep=r",|\t", engine="python", metadata=snap.to_table_metadata(), origin=snap.metadata.origin
                )
                # Identify index columns.
                index_columns = [column for column in _tb.columns if not column[0].isdigit()]
                # Melt the table to have a single "time" column.
                _tb = _tb.melt(id_vars=index_columns, var_name="time", value_name="value")
                # Remove spurious "TIME_PERIOD" from one of the columns.
                _tb = _tb.rename(columns={column: column.replace("\\TIME_PERIOD", "") for column in _tb.columns})
                # Add the dataset code as a column.
                _tb = _tb.assign(**{"dataset_code": dataset_code})
                # Append current table to the list.
                tables.append(_tb)

    # Concatenate all tables.
    tb = pr.concat(tables, ignore_index=True)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(
        [
            "freq",
            "product",
            "nrg_cons",
            "unit",
            "tax",
            "currency",
            "geo",
            "time",
            "dataset_code",
            "nrg_prc",
            "customer",
            "consom",
        ]
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

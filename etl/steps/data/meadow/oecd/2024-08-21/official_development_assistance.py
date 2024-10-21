"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define the list of OECD datasets to upload, the file format, the columns to keep and the index columns.
DATASETS = {
    "dac1": {
        "file_name": "Table1_Data.csv",
        "columns": ["Donor", "PART", "Aid type", "Fund flows", "AMOUNTTYPE", "Year", "Value", "Flags"],
        "index": ["donor", "part", "aid_type", "fund_flows", "amounttype", "year"],
    },
    "dac2a": {
        "file_name": "Table2a_Data.csv",
        "columns": ["Recipient", "Donor", "PART", "Aid type", "AMOUNTTYPE", "Year", "Value", "Flags"],
        "index": ["recipient", "donor", "part", "aid_type", "amounttype", "year"],
    },
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    tables = []
    for dataset, config in DATASETS.items():
        # Retrieve snapshot.
        snap = paths.load_snapshot(f"official_development_assistance_{dataset}.zip")

        # Load data from snapshot.
        tb = snap.read_in_archive(f"{config['file_name']}")

        # Rename DATATYPE column to AMOUNTTYPE.
        if "DATATYPE" in tb.columns:
            tb = tb.rename(columns={"DATATYPE": "AMOUNTTYPE"})

        # Process data.
        tb = tb[config["columns"]].format(config["index"], short_name=dataset)

        # Add table to list.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)  # type: ignore

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

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
    "dac5": {
        "file_name": "Table5_Data.csv",
        "columns": ["Donor", "Sector", "Aid type", "AMOUNTTYPE", "Year", "Value", "Flags"],
        "index": ["donor", "sector", "aid_type", "amounttype", "year"],
    },
}


def run() -> None:
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

        # # Export duplicates to a CSV file.
        # if dataset == "crs":
        #     tb[tb.duplicated(subset=config["index"], keep=False)].to_csv(f"duplicates_{dataset}.csv", index=False)

        # Process data.
        tb = tb[config["columns"]].format(config["index"], short_name=dataset)

        # Add table to list.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)  # type: ignore

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Base year of the OECD's constant-price series. The OECD rebases it at every release (2022, 2023,
# 2024...), and the label below is the only place the base year is stated in the raw files - the
# AMOUNTTYPE code we keep ("D") does not carry it. Garden's metadata declares the same year through
# `definitions.inflation_year`, so when this assertion fires, bump BOTH this constant and
# `inflation_year` in the garden .meta.yml, or every "constant YYYY US$" unit ships mislabelled.
CONSTANT_PRICE_BASE_YEAR = 2024

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

        sanity_check_constant_price_base_year(tb=tb, dataset=dataset)

        # Process data.
        tb = tb[config["columns"]].format(config["index"], short_name=dataset)

        # Add table to list.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)  # ty: ignore

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def sanity_check_constant_price_base_year(tb: Table, dataset: str) -> None:
    """
    Check that the constant-price series is expressed in the base year we declare downstream.

    The `Amount type` label is dropped from the meadow output, so this is the only opportunity to
    read it. All three tables are rebased together, so they must agree with each other too.
    """
    expected = f"Constant Prices ({CONSTANT_PRICE_BASE_YEAR} USD millions)"
    labels = set(tb.loc[tb["AMOUNTTYPE"] == "D", "Amount type"].unique())

    assert labels == {expected}, (
        f"Unexpected constant-price label(s) in {dataset}: {sorted(labels)}. Expected {expected!r}. "
        f"The OECD rebases its constant-price series at each release, so update "
        f"CONSTANT_PRICE_BASE_YEAR here AND `definitions.inflation_year` in the garden .meta.yml."
    )

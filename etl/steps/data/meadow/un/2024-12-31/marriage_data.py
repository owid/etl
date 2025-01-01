"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List of age groups to keep
AGE_GROUPS = ["all", "0-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70+"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("marriage_data.xlsx")

    # Load data from snapshot, starting from the row that contains the header "Country or Area"
    sheet_names = ["MARITAL_STATUS_BY_AGE", "CURRENTLY MARRIED", "EVER_MARRIED", "SMAM"]
    tables = [snap.read(sheet_name=sheet_name, skiprows=2) for sheet_name in sheet_names]

    #
    # Process data.
    #
    # List of columns to select if they exist
    columns_to_select = [
        "Country or area",
        "YearEnd",
        "Sex",
        "AgeGroup",
        "MaritalStatus",
        "DataValue",
        "DataCatalog ShortName",
        "Data Source",
        "Note on Data",
        "DataProcess",
    ]

    processed_tbs = []
    for sheet_name, tb in zip(sheet_names, tables):
        # Select columns if they exist
        existing_columns = [col for col in columns_to_select if col in tb.columns]
        tb = tb[existing_columns]

        # Rename columns
        tb = tb.rename(
            columns={
                "Country or area": "country",
                "AgeGroup": "age",
                "YearEnd": "year",
                "Sex": "sex",
                "MaritalStatus": "marital_status",
                "DataValue": sheet_name.lower(),
                "DataCatalog ShortName": "datacatalog_shortname",
                "Data Source": "data_source",
                "Note on Data": "note_on_data",
                "DataProcess": "data_process",
            }
        )

        if sheet_name == "SMAM":
            # Remove rows with specific text in the note_on_data column
            tb = tb[
                ~tb["note_on_data"].str.contains(
                    "Data presented do not sum up to 100 by more than 0.5 percentage points due to missing values.",
                    na=False,
                )
            ]
        tb = tb.drop(columns=["note_on_data"])

        # Remove square brackets from the values in the AgeGroup column
        if "age" in tb.columns:
            tb["age"] = tb["age"].str.replace(r"\[|\]", "", regex=True)

        # Add "all" to the AgeGroup column if it does not exist
        if "age" not in tb.columns:
            tb["age"] = "all"

        tb = tb[tb["age"].isin(AGE_GROUPS)]

        if "marital_status" in tb.columns:
            tb = tb.pivot(
                index=["country", "year", "age", "sex", "datacatalog_shortname", "data_source", "data_process"],
                columns="marital_status",
                values="marital_status_by_age",
            ).reset_index()

        processed_tbs.append(tb)

    # Merge all processed Tables
    tb_merged = processed_tbs[0]
    for tb in processed_tbs[1:]:
        tb_merged = pr.merge(
            tb_merged,
            tb,
            on=["country", "year", "sex", "age", "datacatalog_shortname", "data_source", "data_process"],
            how="outer",
        )

    # Check if 'currently_married' and 'married' columns are the same
    if "currently_married" in tb_merged.columns and "married" in tb_merged.columns:
        if tb_merged["currently_married"].equals(tb_merged["married"]):
            print("The 'currently_married' and 'married' columns are the same.")
        else:
            print("The 'currently_married' and 'married' columns are different.")

    # Keep only rows where data_source is UNSD
    tb_merged = tb_merged[tb_merged["data_source"] == "UNSD"]

    # Define rules for resolving 'data_process' duplicates
    data_process_rules = [({"Census", "Estimate"}, "Census")]

    subset_columns = ["country", "year", "sex", "age"]

    # Process 'data_process' duplicates
    tb_merged = resolve_duplicates(tb_merged, subset_columns, data_process_rules, target_column="data_process")

    tb_merged = tb_merged.drop(columns=["data_process", "datacatalog_shortname", "data_source"])

    tables = [tb_merged.format(["country", "year", "age", "sex"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def resolve_duplicates(tb, subset_columns, pairing_rules, target_column):
    """
    Resolve duplicates in a Table based on specified column rules.

    Args:
        tb: The table containing duplicates.
        subset_columns (list): List of columns to check for duplicates.
        pairing_rules (list): List of tuples with (set of conflicting values, preferred value).
        target_column (str): Column to apply the rules to (e.g., 'data_source').

    """
    duplicates = tb[tb.duplicated(subset=subset_columns, keep=False)]

    for conflicting_values, preferred_value in pairing_rules:
        # Filter duplicates with the conflicting values
        filtered_duplicates = duplicates[duplicates[target_column].isin(conflicting_values)]
        duplicate_groups = (
            filtered_duplicates.groupby(subset_columns)[target_column]
            .unique()
            .apply(lambda x: conflicting_values.issubset(x))
        )

        # Determine rows to drop
        rows_to_drop = tb[
            tb[target_column].isin(conflicting_values)
            & tb.apply(
                lambda row: (
                    row[target_column] != preferred_value
                    and tuple(row[col] for col in subset_columns) in duplicate_groups[duplicate_groups].index
                ),
                axis=1,
            )
        ].index

        # Drop the identified rows
        tb = tb.drop(rows_to_drop)

    return tb

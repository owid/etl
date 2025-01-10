"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List of age groups to keep
AGE_GROUPS = [
    "all",
    "15-19",
    "20-24",
    "25-29",
    "30-34",
    "35-39",
    "40-44",
    "45-49",
    "50-54" "55-59",
    "60-64",
    "65-69",
    "70-74",
    "75+",
]

DATA_SOURCE_RULES = [
    # Prioritize "DHS_STATcompiler" over "DHS_HH" for cross-country comparability.
    ({"DHS_STATcompiler", "DHS_HH"}, "DHS_STATcompiler"),
    # Prioritize "MICS" over "MICS_HH" for cross-country comparability.
    ({"MICS", "MICS_HH"}, "MICS"),
    # Prioritize "UNSD" over "DHS_STATcompiler" for cross-country comparability.
    ({"DHS_STATcompiler", "UNSD"}, "UNSD"),
    # Prioritize "MICS" over "National statistics" for cross-country comparability.
    ({"National statistics", "MICS"}, "MICS"),
    # Prioritize "UNSD" over "US Census Bureau" for cross-country comparability.
    ({"UNSD", "US Census Bureau"}, "UNSD"),
    # Prioritize "UNSD" over "DHS_HH" for cross-country comparability.
    ({"DHS_HH", "UNSD"}, "UNSD"),
    # Prioritize "UNSD" over "National statistics" for cross-country comparability.
    ({"UNSD", "National statistics"}, "UNSD"),
    # Prioritize "UNSD" over "Eurostat" for cross-country comparability.
    ({"Eurostat", "UNSD"}, "UNSD"),
    # Prioritize "DHS_STATcompiler" over "IPUMS" for cross-country comparability.
    ({"IPUMS", "DHS_STATcompiler"}, "DHS_STATcompiler"),
    # Prioritize "DHS_STATcompiler" over "National statistics" for cross-country comparability.
    ({"DHS_STATcompiler", "National statistics"}, "DHS_STATcompiler"),
    # Prioritize "DHS_STATcompiler" over "INED" for cross-country comparability.
    ({"DHS_STATcompiler", "INED"}, "DHS_STATcompiler"),
    # Prioritize "MICS" over "INED" for standardized indicators.
    ({"INED", "MICS"}, "MICS"),
    # Prioritize "DHS_STATcompiler" over "MICS_HH" for cross-country comparability.
    ({"DHS_STATcompiler", "MICS_HH"}, "DHS_STATcompiler"),
    # Prioritize "DHS_HH" over "National statistics" for cross-country comparability.
    ({"DHS_HH", "National statistics"}, "DHS_HH"),
    # Prioritize "MICS" over "RHS" for broader coverage.
    ({"MICS", "RHS"}, "MICS"),
    # Prioritize "UNSD" over "GGS" for cross-country comparability.
    ({"GGS", "UNSD"}, "UNSD"),
    # Prioritize "MICS_HH" over "National statistics" for cross-country comparability.
    ({"MICS_HH", "National statistics"}, "MICS_HH"),
]
# Define rules for resolving 'data_process' duplicates
DATA_CATALOGUE_RULES = [
    ({"2011 AIS", "2011 DHS"}, "2011 DHS"),  # Broader demographic focus
    ({"2000 HS", "2004 DHS"}, "2004 DHS"),  # More recent data
    ({"2012-2014 DHS", "2014 DHS"}, "2014 DHS"),  # More recent data
    ({"2004 FHS", "2004 HLCS"}, "2004 FHS"),  # More comprehensive health survey
]

DATA_PROCESS_RULES = [
    # Prioritize "Census" over "Estimate" for direct data collection.
    ({"Census", "Estimate"}, "Census"),
    # Prioritize "Census" over "Survey" for better population coverage.
    ({"Census", "Survey"}, "Census"),
    # Prioritize "Survey" over "Estimate" for direct data collection.
    ({"Estimate", "Survey"}, "Survey"),
    # Prioritize "Dual record" over "Survey" for its cross-verificaton.
    ({"Survey", "Dual record"}, "Dual record"),
]


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
        if sheet_name == "EVER_MARRIED":
            tb = tb.rename(columns={"ever_married": "ever_married_total"})

        if sheet_name == "SMAM":
            # Remove rows with specific text in the note_on_data column (they are duplicates of the other rows with no note)
            tb = tb[
                ~tb["note_on_data"].str.contains(
                    "Data presented do not sum up to 100 by more than 0.5 percentage points due to missing values.",
                    na=False,
                )
            ]
        tb = tb.drop(columns=["note_on_data"])

        # Remove square brackets from the values in the age column
        if "age" in tb.columns:
            tb["age"] = tb["age"].str.replace(r"\[|\]", "", regex=True)

        # Add "all" to the age column if it does not exist
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

    # Ensure Laos is consistently named before removing duplicated data
    tb_merged["country"] = tb_merged["country"].str.replace(
        r"Lao People[’']?s (Democratic|Dem\.)? Republic", "Laos", regex=True
    )

    # Define subset columns for removing duplicates
    subset_columns = ["country", "year", "sex", "age"]

    # Get all numeric columns (excluding the grouping columns)
    numeric_columns = tb_merged.select_dtypes(include=["float64", "int64"]).columns

    # Calculate mean for numeric columns and keep first occurrence of non-numeric columns
    agg_dict = {
        col: "mean" if col in numeric_columns else "first" for col in tb_merged.columns if col not in subset_columns
    }

    # Group by subset columns and aggregate
    tb_merged = tb_merged.groupby(subset_columns, as_index=False).agg(agg_dict)

    # # Remove duplicated data different 'datacatalog_shortname' based on the predifined rules
    # tb_merged = resolve_duplicates(
    #     tb_merged, subset_columns, DATA_CATALOGUE_RULES, target_column="datacatalog_shortname"
    # )

    # # Remove duplicated data different 'data_source' based on the predifined rules
    # tb_merged = resolve_duplicates(tb_merged, subset_columns, DATA_SOURCE_RULES, target_column="data_source")

    # # Remove duplicated data different 'data_process' based on the predifined rules
    # tb_merged = resolve_duplicates(tb_merged, subset_columns, DATA_PROCESS_RULES, target_column="data_process")

    # duplicates = tb_merged[tb_merged.duplicated(subset=subset_columns, keep=False)]
    # if not duplicates.empty:
    #     # Check if all duplicates are the same
    #     unique_data = duplicates.groupby(["country", "year", "sex", "age"]).agg(
    #         {"data_source": "unique", "datacatalog_shortname": "unique", "data_process": "unique"}
    #     )
    #     print(unique_data)

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

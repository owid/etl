"""
SCRIPT TO EXTRACT ODA DATA FROM ONE

This code extracts official development assistance (ODA) data from OECD, processed by ONE in specially designed libraries.
oda_data is the name of the wrapper library that provides access to the data.

We are particularly interested in extracting gross disbursements by sectors and channels, only available in the CRS dataset in the OECD data explorer.
The CRS dataset shows aid information by project, with multiple columns not easily understood by non-experts.

Steps to extract the data:
    1. Update the versions of the libraries available in pyproject.toml, if necessary:
        - oda_data
        - oda_reader
        - thefuzz
        - rapidfuzz
        - pandas
    2. Run this code to extract the data.
        python snapshots/one/{version}/oda_data_extract.py
    3. Use the file oda_by_sectors_one.feather as the snapshot, by running the following code:
        python snapshots/one/{version}/official_development_assistance_one.py --path-to-file snapshots/one/{version}/oda_by_sectors_one.feather
    4. Delete all the files that are _not_ oda_by_sectors_one.feather or oda_data_extract.py.


How to run the snapshot on the staging server:

1. sudo apt install libgl1-mesa-glx
2. cd etl
3. uv run pip install oda_data --upgrade
4. uv run python snapshots/one/2024-10-02/oda_data_extract.py

"""


from pathlib import Path
from typing import Dict, List

import pandas as pd
from oda_data import ODAData, set_data_path
from oda_data.tools.groupings import donor_groupings, recipient_groupings
from structlog import get_logger

# Set path of this script
PARENT_DIR = Path(__file__).parent

# Initialize logger.
log = get_logger()

# Set path where ODA data is downloaded
set_data_path(path=PARENT_DIR)

# Save dictionaries of groups of donors and recipients
DONOR_GROUPINGS = donor_groupings()
RECIPIENT_GROUPINGS = recipient_groupings()

# Define parameters in ODAData class
# Define years (remember ranges do not include the last element)
YEARS = range(1960, 2024)

# Define prices: constant or current
PRICES = "constant"

# Set the base year. We must set this given that we've asked for constant data.
BASE_YEAR = 2022

# Define if include names in the data
INCLUDE_NAMES = False

# Define list of indicators to extract
INDICATORS = ["crs_bilateral_flow_disbursement_gross", "imputed_multi_flow_disbursement_gross"]

# Columns to keep
COLUMNS_TO_KEEP = [
    "year",
    "donor_code",
    "recipient_code",
    "sector_code",
    "channel_code",
    "currency",
    "prices",
    "value",
]

# Define columns with codes and names to remap
COLUMNS_TO_REMAP = {
    "donor": ["donor_code", "donor_name"],
    "recipient": ["recipient_code", "recipient_name"],
    "sector": ["sector_code", "sector_name"],
    "channel": ["channel_code", "channel_name"],
}

# Define columns to extract in the csv
FINAL_COLUMNS = [
    "year",
    "donor_code",
    "donor_name",
    "recipient_code",
    "recipient_name",
    "sector_name",
    "channel_code",
    "channel_name",
    "value",
]


def main() -> None:
    df = get_the_data(columns_to_keep=COLUMNS_TO_KEEP)

    df = rename_categories(df=df, columns_to_remap=COLUMNS_TO_REMAP, columns_to_keep=FINAL_COLUMNS)

    df = aggregate_data(df=df, columns_to_keep=FINAL_COLUMNS)

    # Extract data as csv
    df.to_feather(f"{PARENT_DIR}/oda_by_sectors_one.feather")

    log.info("Data extracted successfully")


def get_the_data(columns_to_keep: List[str]) -> pd.DataFrame:
    """
    Get data from ONE wrapper and group it by columns_to_keep
    """

    # Define file name for the raw data
    raw_data_file = f"{PARENT_DIR}/oda_by_sectors_one_raw.feather"

    if Path(raw_data_file).is_file():
        log.info("Raw data already exists. Loading it...")
        return pd.read_feather(raw_data_file)

    log.info("Loading data...")
    # Instantiate the `ODAData` class and store it in a variable called 'oda'
    oda = ODAData(years=YEARS, prices=PRICES, base_year=BASE_YEAR, include_names=INCLUDE_NAMES)

    # Load the indicators
    # A list of all available indicators can be seen by checking `ODAData().available_indicators()`

    # Add all the indicators in our `indicators` list
    for indicator in INDICATORS:
        log.info(f"Loading indicator {indicator}...")
        oda.load_indicator(indicator)

    # Get a DataFrame with all the data.
    df = oda.get_data("all")
    log.info("Raw data loaded to a dataframe")

    # Finally, group the DataFrame rows by year, currency, prices and indicator
    columns_to_group = [col for col in columns_to_keep if col not in ["value"]]
    df = (
        df.groupby(
            columns_to_group,
            observed=True,
            dropna=False,
        )["value"]
        .sum(numeric_only=True)
        .reset_index(drop=False)
    )
    log.info(f"Data grouped by {columns_to_group}")

    # Export grouped data to csv
    df.to_feather(raw_data_file)

    return df


def rename_categories(df: pd.DataFrame, columns_to_remap: Dict[str, List], columns_to_keep: List[str]) -> pd.DataFrame:
    """
    Rename all the categories from codes to names by using crs_codes.json
    """

    # Open the CRS file to obtain the codes and names
    df_crs_full = pd.read_parquet(f"{PARENT_DIR}/fullCRS.parquet")

    for concept, columns in columns_to_remap.items():
        log.info(f"Remapping {concept}...")
        # Keep only the columns we need and drop duplicates
        df_crs_extract = df_crs_full[columns].drop_duplicates(ignore_index=True).copy()

        # Merge the dataframes
        df = pd.merge(df, df_crs_extract, how="left", on=columns[0])

    log.info("Done remapping")

    return df


def aggregate_data(df: pd.DataFrame, columns_to_keep: List[str]) -> pd.DataFrame:
    """
    Create aggregations from definitions of donor and recipient aggregations.
    """
    log.info("Aggregating data...")
    df_donors_list = aggregate_donors(df=df, columns_to_keep=columns_to_keep)
    df_recipients_list = aggregate_recipients(df=df, columns_to_keep=columns_to_keep)

    df_aggregates = pd.concat(df_donors_list + df_recipients_list, ignore_index=True)

    df = pd.concat([df, df_aggregates], ignore_index=True)

    return df


def aggregate_donors(df: pd.DataFrame, columns_to_keep: List[str]) -> List[pd.DataFrame]:
    """
    Create regional aggregations for donors.
    """

    df_donors = df.copy()

    df_donors_list = []

    for donor_group, donor_composition in DONOR_GROUPINGS.items():
        log.info(f"Aggregating {donor_group} donors...")
        df_donors_by_group = df_donors[df_donors["donor_code"].isin(donor_composition.keys())].copy()

        df_donors_by_group = (
            df_donors_by_group.groupby(
                [col for col in columns_to_keep if col not in ["donor_code", "donor_name", "value"]],
                observed=True,
                dropna=False,
            )["value"]
            .sum()
            .reset_index(drop=False)
        )

        df_donors_by_group["donor_name"] = donor_group

        df_donors_list.append(df_donors_by_group)

        for recipient_group, recipient_composition in RECIPIENT_GROUPINGS.items():
            log.info(f"Aggregating {donor_group} donors to {recipient_group} recipients...")
            df_donors_by_recipient_group = df_donors_by_group[
                df_donors_by_group["recipient_code"].isin(recipient_composition.keys())
            ].copy()

            df_donors_by_recipient_group = (
                df_donors_by_recipient_group.groupby(
                    [
                        col
                        for col in columns_to_keep
                        if col not in ["donor_code", "recipient_code", "recipient_name", "value"]
                    ],
                    observed=True,
                    dropna=False,
                )["value"]
                .sum()
                .reset_index(drop=False)
            )

            df_donors_by_recipient_group["recipient_name"] = recipient_group

            df_donors_list.append(df_donors_by_recipient_group)

    return df_donors_list


def aggregate_recipients(df: pd.DataFrame, columns_to_keep: List[str]) -> List[pd.DataFrame]:
    """
    Create regional aggregations for recipients.
    """

    df_recipients = df.copy()

    df_recipients_list = []

    for recipient_group, recipient_composition in RECIPIENT_GROUPINGS.items():
        log.info(f"Aggregating {recipient_group} recipients...")
        df_recipients_by_group = df_recipients[
            df_recipients["recipient_code"].isin(recipient_composition.keys())
        ].copy()

        df_recipients_by_group = (
            df_recipients_by_group.groupby(
                [col for col in columns_to_keep if col not in ["recipient_code", "recipient_name", "value"]],
                observed=True,
                dropna=False,
            )["value"]
            .sum()
            .reset_index(drop=False)
        )

        df_recipients_by_group["recipient_name"] = recipient_group

        df_recipients_list.append(df_recipients_by_group)

    return df_recipients_list


if __name__ == "__main__":
    main()

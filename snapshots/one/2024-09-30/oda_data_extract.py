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

"""


from pathlib import Path
from typing import Dict, List

import pandas as pd
from oda_data import ODAData, set_data_path
from oda_data.tools.groupings import donor_groupings, recipient_groupings

# Set path of this script
PARENT_DIR = Path(__file__).parent

# Set path where ODA data is downloaded
set_data_path(path=PARENT_DIR)

# Save dictionaries of available donors and recipients
AVAILABLE_DONORS = ODAData().available_donors()
AVAILABLE_RECIPIENTS = ODAData().available_recipients()

DONOR_GROUPINGS = donor_groupings()
RECIPIENT_GROUPINGS = recipient_groupings()

# print(DONOR_GROUPINGS)

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
    "indicator",
    "sector_code",
    "purpose_code",
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
    "purpose": ["purpose_code", "purpose_name"],
    "channel": ["channel_code", "channel_name", "parent_channel_code", "channel_reported_name"],
}


def main() -> None:
    df = get_the_data(columns_to_keep=COLUMNS_TO_KEEP)

    df = aggregate_data(df=df)

    df = rename_categories(df=df, columns_to_remap=COLUMNS_TO_REMAP)

    # Extract data as csv
    df.to_csv(f"{PARENT_DIR}/oda_by_sectors_oda.csv", index=False)


def get_the_data(columns_to_keep: List[str]) -> pd.DataFrame:
    # Instantiate the `ODAData` class and store it in a variable called 'oda'
    oda = ODAData(years=YEARS, prices=PRICES, base_year=BASE_YEAR, include_names=INCLUDE_NAMES)

    # Load the indicators
    # A list of all available indicators can be seen by checking `ODAData().available_indicators()`

    # Add all the indicators in our `indicators` list
    for indicator in INDICATORS:
        oda.load_indicator(indicator)

    # Get a DataFrame with all the data.
    df = oda.get_data("all")

    for col in df.columns:
        print(col)

    # Finally, group the DataFrame rows by year, currency, prices and indicator
    df = (
        df.groupby(
            [col for col in columns_to_keep if col != "value"],
            observed=True,
            dropna=False,
        )["value"]
        .sum(numeric_only=True)
        .reset_index(drop=False)
    )

    return df


def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create aggregations from definitions of donor and recipient aggregations.
    """
    # Create regional aggregations for this data
    df_donors = df.copy()
    df_recipients = df.copy()

    return df


def rename_categories(df: pd.DataFrame, columns_to_remap: Dict[str, List]) -> pd.DataFrame:
    """
    Rename all the categories from codes to names by using crs_codes.json
    """

    # Open the CRS file to obtain the codes and names
    df_crs_full = pd.read_parquet(f"{PARENT_DIR}/fullCRS.parquet")

    for concept, columns in columns_to_remap.items():
        # Keep only the columns we need and drop duplicates
        df_crs_extract = df_crs_full[columns].drop_duplicates().copy()

        # Merge the dataframes
        df = pd.merge(df, df_crs_extract, how="left", on=columns[0])

    for col in df_crs_full.columns:
        print(col)

    return df


if __name__ == "__main__":
    main()

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
    3. Use the file oda_by_sectors_one.feather as the snapshot, by running the following codes:
        python snapshots/one/{version}/oda_one_sectors.py --path-to-file snapshots/one/{version}/oda_one_sectors.feather
        python snapshots/one/{version}/oda_one_channels.py --path-to-file snapshots/one/{version}/oda_one_channels.feather
    4. Delete oda_one_sectors.feather, oda_one_channels.feather and fullCRS.parquet files.


How to run the snapshot on the staging server:

1. sudo apt install libgl1-mesa-glx
2. cd etl
3. uv run pip install oda_data --upgrade
4. uv run python "snapshots/one/2024-10-02/oda_data_extract.py"


# To run it and save a log file:
nohup uv run python "snapshots/one/2024-10-02/oda_data_extract.py" > output_one.log 2>&1 &

"""

from pathlib import Path
from typing import List

import pandas as pd
from oda_data import read_crs, set_data_path
from oda_data.tools.groupings import donor_groupings, recipient_groupings
from structlog import get_logger

# Set path of this script
PARENT_DIR = Path(__file__).parent

# Initialize logger.
log = get_logger()

# Set path where ODA data is downloaded
set_data_path(path=PARENT_DIR)

# Define start and end years for the data
START_YEAR = 1960
END_YEAR = 2024

# Save dictionaries of groups of donors and recipients
DONOR_GROUPINGS = donor_groupings()
RECIPIENT_GROUPINGS = recipient_groupings()


# Define index + value columns
BASIC_COLUMNS = [
    "year",
    "donor_code",
    "donor_name",
    "recipient_code",
    "recipient_name",
    "value",
]


def main() -> None:
    # Define the start and end years for the data

    # Get the data by sector and channels
    df_sector = get_data_by_sector_or_channel(
        start_year=START_YEAR,
        end_year=END_YEAR,
        flow_column="usd_commitment_constant",
        sector_or_channel="sector",
    )
    df_channel = get_data_by_sector_or_channel(
        start_year=START_YEAR,
        end_year=END_YEAR,
        flow_column="usd_disbursement_constant",
        sector_or_channel="channel",
    )

    # Save the data to feather files
    df_sector.to_feather(f"{PARENT_DIR}/oda_one_sectors.feather")
    df_channel.to_feather(f"{PARENT_DIR}/oda_one_channels.feather")


def get_data_by_sector_or_channel(
    start_year: int,
    end_year: int,
    flow_column: str = "usd_commitment_constant",
    by_donor: bool = True,
    by_recipient: bool = True,
    sector_or_channel: str = "sector",
) -> pd.DataFrame:
    """Get the data by sector.
    Args:
        start_year (int): The start year for the data.
        end_year (int): The end year for the data.
        flow_column (str): The flow column to aggregate (e.g usd_disbursement_constant, usd_commitment_constant).
        by_donor (bool): Whether to show the data by donor country. The default is True,
        by_recipient (bool): Whether to show the data by recipient country. The default is True,
        which means the data will be shown for all recipients, total.
    Returns:
        pd.DataFrame: A DataFrame with data by sector.
    """

    # Create an instance of the ODAData class. For the sectors analysis, the starting
    # year must be 2 years before the requested start_year.
    crs = read_crs(years=list(range(start_year, end_year + 1)))

    # filter for ODA
    crs = crs.loc[lambda d: d.category == 10]

    # Select columns depending on sector_or_channel
    if sector_or_channel == "sector":
        extra_columns = ["sector_name"]
    elif sector_or_channel == "channel":
        extra_columns = ["channel_code"]

    # Define columns to keep
    columns_to_keep = (
        [
            "year",
            "donor_code",
            "donor_name",
            "recipient_code",
            "recipient_name",
        ]
        + [flow_column]
        + extra_columns
    )

    # Get the spending data
    spending = crs.filter(columns_to_keep)

    # group data
    grouper = [
        c
        for c in columns_to_keep
        if c
        not in [
            "donor_code",
            "donor_name",
            "recipient_code",
            "recipient_name",
            flow_column,
        ]
    ]

    if by_donor:
        grouper.extend(["donor_code", "donor_name"])

    if by_recipient:
        grouper.extend(["recipient_code", "recipient_name"])

    spending = spending.groupby(grouper, observed=True, dropna=False)[flow_column].sum().reset_index()

    # Rename flow_column by value
    spending = spending.rename(columns={flow_column: "value"})

    # Replace flow_column by value in columns_to_keep
    columns_to_keep = [col if col != flow_column else "value" for col in columns_to_keep]

    spending = aggregate_data(df=spending, columns_to_keep=columns_to_keep)

    return spending


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

        # Check missing donors in the data
        missing_donors = set(donor_composition.keys()).difference(df_donors["donor_code"].unique())

        # Construct a list of names of missing donors from the codes of missing_donors
        missing_donors_names = [v for k, v in donor_composition.items() if k in missing_donors]

        if missing_donors:
            log.warning(f"Missing donors in the data for {donor_group}: {missing_donors_names}")

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
                        if col
                        not in [
                            "donor_code",
                            "recipient_code",
                            "recipient_name",
                            "value",
                        ]
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

        # Check missing recipients in the data
        missing_recipients = set(recipient_composition.keys()).difference(df_recipients["recipient_code"].unique())

        # Construct a list of names of missing recipients from the codes of missing_recipients
        missing_recipients = [v for k, v in recipient_composition.items() if k in missing_recipients]

        if missing_recipients:
            log.warning(f"Missing recipients in the data for {recipient_group}: {missing_recipients}")

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

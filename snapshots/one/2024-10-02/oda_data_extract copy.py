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


import pandas as pd
from oda_data import read_crs, set_data_path

from scripts import config

# Set the path to the raw data folder. Using the config module, we can access the "raw_data" folder
# inside this project's root folder.
set_data_path(config.Paths.raw_data)

# Define start and end years for the data
START_YEAR = 1973
END_YEAR = 2024


def main() -> None:
    # Define the start and end years for the data

    # Get the data by sector
    data = get_data_by_sector(START_YEAR, END_YEAR)

    print(data)

    # Save the data to a feather file
    data.to_feather(config.Paths.oda_by_sectors_one)


def get_data_by_sector(
    start_year: int,
    end_year: int,
    flow_column: str = "usd_disbursement",
    by_donor: bool = False,
    by_recipient: bool = False,
) -> pd.DataFrame:
    """Get the data by sector.
    Args:
        start_year (int): The start year for the data.
        end_year (int): The end year for the data.
        flow_column (str): The flow column to aggregate (e.g usd_disbursement, usd_commitment).
        by_donor (bool): Whether to show the data by donor country. The default is False,
        by_recipient (bool): Whether to show the data by recipient country. The default is False,
        which means the data will be shown for all recipients, total.
    Returns:
        pd.DataFrame: A DataFrame with data by sector.
    """

    # Create an instance of the ODAData class. For the sectors analysis, the starting
    # year must be 2 years before the requested start_year.
    crs = read_crs(years=list(range(start_year, end_year + 1)))

    # filter for ODA
    crs = crs.loc[lambda d: d.category == 10]

    # Get the spending data
    spending = crs.filter(
        [
            "year",
            "indicator",
            "donor_code",
            "donor_name",
            "recipient_code",
            "recipient_name",
            "purpose_code",
            "prices",
            "currency",
            flow_column,
        ]
    )

    # group data
    grouper = [
        c
        for c in spending.columns
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

    return spending


if __name__ == "__main__":
    main()

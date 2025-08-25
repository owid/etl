"""Script to create a snapshot of dataset.

This snapshot processes multiple zip files from CPUC containing deployment reports from 2022-2025,
extracting CSV files that contain TotalPMT (Total Passenger Miles Traveled) data.
"""

import re
import tempfile
import zipfile
from pathlib import Path
from typing import List

import pandas as pd
import requests

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# List of all zip URLs to download and process
ZIP_URLS = [
    "https://www.cpuc.ca.gov/-/media/cpuc-website/files/uploadedfiles/cpucwebsite/content/licensing/autovehicle/2022-av-deployment---0901-1130.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/files/uploadedfiles/cpucwebsite/content/licensing/autovehicle/2022-av-deployment--0601-0831-revised.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/files/uploadedfiles/cpucwebsite/content/licensing/autovehicle/2022-av-deployment--0301-0531-revised.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/2023-av-deployment---0901-1130(2).zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/2023-av-deployment---0601-0831.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/2023-av-deployment---0301-0531/2023-av-deployment---0301-0531.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/files/uploadedfiles/cpucwebsite/content/licensing/autovehicle/2023-av-deployment---1201-0228.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/waymo-dep-dec-2024--public_2.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/waymo-dep-sep-nov-2024-public_2a.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/av-deployment_jun_aug_2024_public.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/2024-av-deployment---0301-0531.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/2024-av-deployment---1201-0229(1).zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/waymo-driverless-deployment-2025-q1.zip",
    "https://www.cpuc.ca.gov/-/media/cpuc-website/divisions/consumer-protection-and-enforcement-division/documents/tlab/av-programs/waymo-deployment-2025q2.zip",
]


def run(upload: bool = True) -> None:
    """Create a new snapshot."""
    # Download and extract CSV files with TotalPMT column
    dataframes = download_and_extract_csv_files(ZIP_URLS)

    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)
    snap = paths.init_snapshot()

    # Create a temporary CSV file with the combined data
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "robotaxis.csv"
        combined_df.to_csv(output_file, index=False)
        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload, filename=output_file)


def download_and_extract_csv_files(urls: List[str]) -> List[pd.DataFrame]:
    """Download zip files and extract CSV files that contain TotalPMT column.

    Args:
        urls: List of URLs to zip files to download

    Returns:
        List of DataFrames from CSV files containing TotalPMT column
    """
    all_dataframes = []

    for url in urls:
        print(f"Processing {url}")

        try:
            # Download the zip file
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Create temporary directory for extraction
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Save and extract zip file
                zip_path = temp_path / "data.zip"
                zip_path.write_bytes(response.content)

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(temp_path)

                # Find all CSV files recursively
                csv_files = list(temp_path.rglob("*.csv"))

                for csv_file in csv_files:
                    try:
                        df = pd.read_csv(csv_file)

                        if "TotalPMT" in df.columns:
                            print(f"Found TotalPMT in {csv_file.name}")

                            # Check if TotalPMT column has actual values
                            if df["TotalPMT"].replace("", pd.NA).replace(r"^\s*$", pd.NA, regex=True).dropna().empty:
                                print(f"Skipping {csv_file.name} - TotalPMT column has no values")
                                continue

                            # Extract only required columns if they exist
                            required_cols = ["Year", "Month", "TotalTrips", "TotalPassengersCarried", "TotalPMT"]
                            available_cols = [col for col in required_cols if col in df.columns]

                            df = df[available_cols].copy()

                            # Clean empty strings and whitespace
                            df = df.replace("", pd.NA).replace(r"^\s*$", pd.NA, regex=True)

                            # Remove rows with NaNs in key columns
                            key_cols = ["Year", "Month", "TotalTrips", "TotalPassengersCarried", "TotalPMT"]
                            cols_to_check = [col for col in key_cols if col in df.columns]
                            df = df.dropna(subset=cols_to_check)

                            # Skip if no data remains after cleaning
                            if df.empty:
                                print(f"Skipping {csv_file.name} - no valid data after removing NaNs")
                                continue

                            # Extract TCPID from filename if missing
                            if "TCPID" not in df.columns:
                                tcpid_match = re.search(r"(PSG\d+)", csv_file.name)
                                if tcpid_match:
                                    df["TCPID"] = tcpid_match.group(1)
                                    print(f"Added TCPID {tcpid_match.group(1)} from filename")

                            # Add source file info for tracking
                            df["source_file"] = csv_file.name
                            df["source_url"] = url
                            all_dataframes.append(df)

                    except Exception as e:
                        print(f"Error reading {csv_file.name}: {e}")
                        continue

        except Exception as e:
            print(f"Error processing {url}: {e}")
            continue

    return all_dataframes

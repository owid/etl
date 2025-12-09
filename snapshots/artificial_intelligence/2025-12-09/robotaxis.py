"""Script to create a snapshot of dataset.

This snapshot processes multiple zip files from CPUC containing deployment reports from 2022-2025,
extracting CSV files that contain TotalPMT (Total Passenger Miles Traveled) data.
"""

import re
import tempfile
import zipfile
from pathlib import Path
from typing import List, cast
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# CPUC quarterly reporting page URL
CPUC_URL = "https://www.cpuc.ca.gov/regulatory-services/licensing/transportation-licensing-and-analysis-branch/autonomous-vehicle-programs/quarterly-reporting"


def extract_deployment_links() -> List[str]:
    """Extract all deployment program zip file URLs from CPUC website.

    Returns:
        List of URLs to deployment program zip files
    """
    response = requests.get(CPUC_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    base_url = "https://www.cpuc.ca.gov"

    links = []
    for link in soup.find_all("a", href=True):
        if not isinstance(link, Tag):
            continue
        href = link.get("href")
        if href:
            href = str(href)
            # Filter for deployment program zip files
            if ("deployment" in href.lower() or "waymo-dep" in href.lower()) and ".zip" in href.lower():
                full_url = urljoin(base_url, href)
                if full_url not in links:
                    links.append(full_url)

    return links


def run(upload: bool = True) -> None:
    """Create a new snapshot."""
    # Extract deployment program URLs dynamically from CPUC website
    zip_urls = extract_deployment_links()
    # Download and extract CSV files with TotalPMT column
    dataframes = download_and_extract_csv_files(zip_urls)

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

            # Check for nested zip files and extract them too
            nested_zips = list(temp_path.rglob("*.zip"))
            for nested_zip in nested_zips:
                nested_extract_path = nested_zip.parent / nested_zip.stem
                nested_extract_path.mkdir(exist_ok=True)
                with zipfile.ZipFile(nested_zip, "r") as nested_ref:
                    nested_ref.extractall(nested_extract_path)

            # Find all CSV files recursively (including from nested zips)
            csv_files = list(temp_path.rglob("*.csv"))

            for csv_file in csv_files:
                df = pd.read_csv(csv_file, low_memory=False)

                if "TotalPMT" in df.columns:
                    # Check if TotalPMT column has actual values
                    if df["TotalPMT"].replace("", pd.NA).replace(r"^\s*$", pd.NA, regex=True).dropna().empty:
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
                        continue

                    # Extract TCPID from filename if missing
                    if "TCPID" not in df.columns:
                        tcpid_match = re.search(r"(PSG\d+)", csv_file.name)
                        if tcpid_match:
                            df["TCPID"] = tcpid_match.group(1)

                    # Add source file info for tracking
                    df["source_file"] = csv_file.name
                    df["source_url"] = url
                    all_dataframes.append(df)

    return all_dataframes

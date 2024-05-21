import os
import time
import zipfile
from typing import Any, List

import requests


def compress_files(file_paths: List[str], zip_file_path: str) -> None:
    with zipfile.ZipFile(zip_file_path, "w") as zipf:
        for file_path in file_paths:
            zipf.write(file_path, os.path.basename(file_path))


def download_data(file_number: int, tmpdirname: Any, base_url: str) -> str:
    # Unique URL for each file
    url_to_download = f"{base_url}{file_number}.zip"
    # Retry logic
    max_retries = 5
    backoff_factor = 1  # Factor for exponential backoff

    for attempt in range(max_retries):
        try:
            response = requests.get(url_to_download)
            response.raise_for_status()
            break  # If request is successful, exit the loop
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = backoff_factor * (2**attempt)  # Exponential backoff
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Failed to download the file after {max_retries} attempts. Error: {e}")
                raise
    # Download data from source, open the csv within and return that.
    response = requests.get(url_to_download)

    zip_file_name = f"{base_url.split('/')[-1]}{file_number}.zip"
    zip_file_path = os.path.join(tmpdirname, zip_file_name)
    with open(zip_file_path, "wb") as f:
        f.write(response.content)
    # Extract the zip file
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(tmpdirname)
    # Construct the CSV file name and path
    csv_file_name = f"{base_url.split('/')[-1]}{file_number}.csv"
    csv_file_path = os.path.join(tmpdirname, csv_file_name)
    return csv_file_path

import time
import zipfile
from io import BytesIO

import pandas as pd
import requests
from owid.repack import repack_frame


def download_data(file_number: int, base_url: str) -> pd.DataFrame:
    # Unique URL for each file
    url_to_download = f"{base_url}{file_number}.zip"
    csv_file_name = f"{base_url.split('/')[-1]}{file_number}.csv"

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

    # Load the ZIP file into a BytesIO object
    zip_file = BytesIO(response.content)

    # Read the CSV file from the ZIP file
    with zipfile.ZipFile(zip_file, "r") as z:
        with z.open(csv_file_name) as f:
            df = pd.read_csv(f)

    # Use smaller types
    df = repack_frame(df)

    return df

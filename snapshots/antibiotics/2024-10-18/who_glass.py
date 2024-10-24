"""Script to create a snapshot of dataset."""

import os
import tempfile
import time
import zipfile
from pathlib import Path

import click
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()
# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/who_glass.zip")
    file_path = get_shiny_data()

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, filename=file_path)
    os.remove(file_path)


def get_shiny_data() -> str:
    """
    Get data from https://worldhealthorg.shinyapps.io/glass-dashboard/_w_679389fb/#!/amr
    Specifically - Global maps of testing coverage by infectious syndrome.
    This script downloads data for multiple years and syndromes and stores them in a zip file.
    """

    years = range(2016, 2023)
    syndromes = ["URINE", "BLOOD", "STOOL", "UROGENITAL"]

    # Set up the driver (ensure you have ChromeDriver or another driver installed)
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 20)

    with tempfile.TemporaryDirectory() as temp_dir:
        for syndrome in syndromes:
            log.info(f"Downloading data for syndrome: {syndrome}")
            for year in years:
                log.info(f"Downloading data for year: {year}")

                # Open the webpage
                driver.get("https://worldhealthorg.shinyapps.io/glass-dashboard/_w_679389fb/#!/amr")

                # Scroll to the section where the dropdowns are located
                section = wait.until(EC.presence_of_element_located((By.ID, "plot-amr-3")))
                time.sleep(1)
                driver.execute_script("arguments[0].scrollIntoView(true);", section)
                time.sleep(1)
                # Wait for the year dropdown to become visible and interactable
                year_dropdown = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="amr-gc_infsys-year-select-selectized"]'))
                )
                driver.execute_script("arguments[0].click();", year_dropdown)
                time.sleep(1)

                # Select the year
                option_year = wait.until(EC.element_to_be_clickable((By.XPATH, f'//div[@data-value="{year}"]')))
                driver.execute_script("arguments[0].click();", option_year)
                time.sleep(1)

                # Wait for the syndrome dropdown to become visible and interactable
                syndrome_dropdown = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="amr-gc_infsys-infsys-select-selectized"]'))
                )
                driver.execute_script("arguments[0].click();", syndrome_dropdown)

                # Select the syndrome
                option_syndrome = wait.until(EC.element_to_be_clickable((By.XPATH, f'//div[@data-value="{syndrome}"]')))
                driver.execute_script("arguments[0].click();", option_syndrome)

                # Trigger the download button
                download_link_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="amr-gc_infsys-dl-data"]'))
                )

                # Get the href attribute of the download link
                download_link = download_link_element.get_attribute("href")

                if download_link:
                    response = requests.get(download_link)
                    file_path = os.path.join(temp_dir, f"{syndrome}_{year}.csv")
                    # Save the CSV content to a file in the temporary directory
                    with open(file_path, "wb") as file:
                        file.write(response.content)
                    log.info(f"Downloaded {syndrome}_{year}.csv to {file_path}")
                else:
                    log.error(f"No download link found for {syndrome} in {year}.")

        # Zip all downloaded files
        zip_file_path = "downloaded_data.zip"
        with zipfile.ZipFile(zip_file_path, "w") as zipf:
            for foldername, subfolders, filenames in os.walk(temp_dir):
                for filename in filenames:
                    file_path = os.path.join(foldername, filename)
                    zipf.write(file_path, os.path.basename(file_path))

    driver.quit()
    return zip_file_path


if __name__ == "__main__":
    main()

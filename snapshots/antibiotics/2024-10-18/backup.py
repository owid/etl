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
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/who_glass_by_antibiotic.zip")
    file_path = get_shiny_data()

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, filename=file_path)
    os.remove(file_path)


def get_shiny_data() -> str:
    """
    Get data from https://worldhealthorg.shinyapps.io/glass-dashboard/_w_679389fb/#!/amr
    Specifically - Global maps of testing coverage by bacterial pathogen and antibiotic group
    This script downloads data for multiple years and syndromes and stores them in a zip file.
    """

    years = range(2016, 2023)
    drop_down_dict = {
        "BLOOD": {
            "Acinetobacter spp.": ["Carbapenems"],
            "Escherichia coli": ["Carbapenems", "Third-generation cephalosporins"],
            "Klebsiella pneumoniae": ["Third-generation cephalosporins", "Carbapenems"],
            "Staphylococcus aureus": ["Methicillin-resistance"],
            "Streptococcus pneumoniae": ["Penicillins"],
        },
        "STOOL": {
            "Salmonella spp.": ["Fluoroquinolones"],
            "Shigella spp.": ["Third-generation cephalosporins"],
        },
        "UROGENITAL": {
            "Neisseria gonorrhoeae": ["Macrolides", "Third-generation cephalosporins"],
        },
        "URINE": {
            "Escherichia coli": [
                "Fluoroquinolones",
                "Sulfonamides and trimethoprim",
                "Third-generation cephalosporins",
            ],
            "Klebsiella pneumoniae": [
                "Fluoroquinolones",
                "Sulfonamides and trimethoprim",
                "Third-generation cephalosporins",
            ],
        },
    }

    # Set up the driver (ensure you have ChromeDriver or another driver installed)
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 20)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Open the webpage
        driver.get("https://worldhealthorg.shinyapps.io/glass-dashboard/_w_679389fb/#!/amr")

        # Scroll to the section where the dropdowns are located
        section = wait.until(EC.presence_of_element_located((By.ID, "plot-amr-6")))
        time.sleep(1)
        driver.execute_script("arguments[0].scrollIntoView(true);", section)
        time.sleep(1)

        for syndrome in drop_down_dict.keys():
            log.info(f"Downloading data for syndrome: {syndrome}")

            # Scroll to the dropdown section first
            driver.execute_script("arguments[0].scrollIntoView(true);", section)

            # Click on the syndrome dropdown and select the syndrome
            syndrome_dropdown = wait.until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="amr-gc_pathogen_anti-infsys-select-selectized"]'))
            )
            driver.execute_script("arguments[0].click();", syndrome_dropdown)
            option_syndrome = wait.until(EC.element_to_be_clickable((By.XPATH, f'//div[@data-value="{syndrome}"]')))
            driver.execute_script("arguments[0].click();", option_syndrome)
            time.sleep(1)

            for pathogen in drop_down_dict[syndrome].keys():
                log.info(f"Downloading data for pathogen: {pathogen}")

                # Click on the pathogen dropdown and select the pathogen
                pathogen_dropdown = wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, '//*[@id="amr-gc_pathogen_anti-pathogen-select-selectized"]')
                    )
                )
                # Reset any previous selection in the dropdown
                driver.execute_script("arguments[0].click();", pathogen_dropdown)
                option_pathogen = wait.until(EC.element_to_be_clickable((By.XPATH, f'//div[@data-value="{pathogen}"]')))
                driver.execute_script("arguments[0].click();", option_pathogen)
                time.sleep(1)  # Wait to ensure the UI has updated

                for antibiotic_group in drop_down_dict[syndrome][pathogen]:
                    log.info(f"Downloading data for antibiotic group: {antibiotic_group}")

                    # Click on the antibiotic group dropdown and select the antibiotic group
                    group_dropdown = wait.until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//*[@id="amr-gc_pathogen_anti-antibiotic-select-selectized"]')
                        )
                    )
                    driver.execute_script("arguments[0].click();", group_dropdown)
                    option_group = wait.until(
                        EC.element_to_be_clickable((By.XPATH, f'//div[@data-value="{antibiotic_group}"]'))
                    )
                    driver.execute_script("arguments[0].click();", option_group)
                    time.sleep(1)  # Ensure the dropdown has reset before next iteration

                    for year in years:
                        log.info(f"Downloading data for year: {year}")

                        # Click on the year dropdown and select the year
                        year_dropdown = wait.until(
                            EC.presence_of_element_located(
                                (By.XPATH, '//*[@id="amr-gc_pathogen_anti-year-select-label"]')
                            )
                        )
                        driver.execute_script("arguments[0].click();", year_dropdown)
                        option_year = wait.until(EC.element_to_be_clickable((By.XPATH, f'//div[@data-value="{year}"]')))
                        driver.execute_script("arguments[0].click();", option_year)
                        time.sleep(1)

                        # Trigger the download button
                        download_link_element = wait.until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="amr-gc_pathogen_anti-dl-data"]'))
                        )

                        # Get the href attribute of the download link
                        download_link = download_link_element.get_attribute("href")

                        if download_link:
                            response = requests.get(download_link)
                            file_path = os.path.join(temp_dir, f"{syndrome}_{antibiotic_group}_{pathogen}_{year}.csv")
                            # Save the CSV content to a file in the temporary directory
                            with open(file_path, "wb") as file:
                                file.write(response.content)
                            log.info(f"Downloaded {syndrome}_{antibiotic_group}_{pathogen}_{year}.csv to {file_path}")
                        else:
                            log.error(f"No download link found for {syndrome}, {antibiotic_group}, {pathogen}, {year}.")
                    # driver.refresh()
                    # time.sleep(3)

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

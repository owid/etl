"""Script to create a snapshot of dataset."""

import os
import tempfile
import time
import zipfile
from pathlib import Path

import click
import requests
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
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
            "Carbapenems": ["Klebsiella pneumoniae", "Escherichia coli", "Acinetobacter spp."],
            "Fluoroquinolones": ["Salmonella spp."],
            "Methicillin-resistance": ["Staphylococcus aureus"],
            "Penicillins": ["Streptococcus pneumoniae"],
        },
        "STOOL": {
            "Fluoroquinolones": ["Salmonella spp."],
            "Third-generation cephalosporins": ["Shigella spp."],
        },
        "UROGENITAL": {
            "Macrolides": ["Neisseria gonorrhoeae"],
            "Third-generation cephalosporins": ["Neisseria gonorrhoeae"],
        },
        "URINE": {
            "Fluoroquinolones": ["Escherichia coli", "Klebsiella pneumoniae"],
            "Sulfonamides and trimethoprim": ["Escherichia coli", "Klebsiella pneumoniae"],
            "Third-generation cephalosporins": ["Escherichia coli", "Klebsiella pneumoniae"],
        },
    }

    # Set up the driver (ensure you have ChromeDriver or another driver installed)
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 20)
    actions = ActionChains(driver)  # Initialize ActionChains

    with tempfile.TemporaryDirectory() as temp_dir:
        # Open the webpage
        driver.get("https://worldhealthorg.shinyapps.io/glass-dashboard/_w_679389fb/#!/amr")

        # Scroll to the section where the dropdowns are located
        section = wait.until(EC.presence_of_element_located((By.ID, "plot-amr-6")))
        time.sleep(1)
        driver.execute_script("arguments[0].scrollIntoView(true);", section)
        time.sleep(1)
        for syndrome in drop_down_dict.keys():
            # log.info(f"Downloading data for syndrome: {syndrome}")

            log.info(f"Downloading data for syndrome: {syndrome}")
            # Wait for the syndrome dropdown to become visible and interactable

            # Scroll to the dropdown section first
            section = wait.until(EC.presence_of_element_located((By.ID, "plot-amr-6")))
            driver.execute_script("arguments[0].scrollIntoView(true);", section)

            # Wait for the dropdown to become visible for URINE
            dropdown = wait.until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        f'//input[@id="amr-gc_pathogen_anti-infsys-select-selectized"]/preceding-sibling::div[@data-value="{syndrome}"]',
                    )
                )
            )

            # Use ActionChains to interact with the element
            actions = ActionChains(driver)
            actions.move_to_element(dropdown).click().perform()

            # Debugging: Ensure the element was clicked
            for antibiotic_group in drop_down_dict[syndrome].keys():
                log.info(f"Downloading data for antibiotic group: {antibiotic_group}")
                group_dropdown = wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, '//*[@id="amr-gc_pathogen_anti-antibiotic-select-selectized"]')
                    )
                )
                actions.move_to_element(group_dropdown).click().perform()
                time.sleep(1)

                # Select the antibiotic group
                option_group = wait.until(
                    EC.element_to_be_clickable((By.XPATH, f'//div[@data-value="{antibiotic_group}"]'))
                )
                actions.move_to_element(option_group).click().perform()
                time.sleep(1)

                for pathogen in drop_down_dict[syndrome][antibiotic_group]:
                    time.sleep(3)
                    log.info(f"Downloading data for pathogen: {pathogen}")

                    # Scroll to the pathogen dropdown if necessary
                    pathogen_dropdown = wait.until(
                        EC.visibility_of_element_located(
                            (By.XPATH, '//*[@id="amr-gc_pathogen_anti-pathogen-select-selectized"]')
                        )
                    )
                    driver.execute_script("arguments[0].scrollIntoView(true);", pathogen_dropdown)

                    # Use JavaScript to click on the pathogen dropdown
                    driver.execute_script("arguments[0].click();", pathogen_dropdown)
                    time.sleep(1)  # Give time for the dropdown options to load

                    # Select the pathogen
                    option_pathogen = wait.until(
                        EC.element_to_be_clickable((By.XPATH, f'//div[@data-value="{pathogen}"]'))
                    )
                    driver.execute_script("arguments[0].scrollIntoView(true);", option_pathogen)  # Ensure visibility
                    driver.execute_script(
                        "arguments[0].click();", option_pathogen
                    )  # Click using JS to avoid any click interception issues
                    time.sleep(1)

                    for year in years:
                        log.info(f"Downloading data for year: {year}")

                        # Wait for the year dropdown to become visible and interactable
                        year_dropdown = wait.until(
                            EC.presence_of_element_located(
                                (By.XPATH, '//*[@id="amr-gc_pathogen_anti-year-select-label"]')
                            )
                        )
                        actions.move_to_element(year_dropdown).click().perform()
                        time.sleep(1)

                        # Select the year
                        option_year = wait.until(EC.element_to_be_clickable((By.XPATH, f'//div[@data-value="{year}"]')))
                        actions.move_to_element(option_year).click().perform()
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

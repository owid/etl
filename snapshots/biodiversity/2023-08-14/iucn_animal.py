import os
import shutil
import time
from pathlib import Path

import click
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"biodiversity/{SNAPSHOT_VERSION}/iucn_animal.csv")
    chrome_options = Options()
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": os.path.dirname(snap.path),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )

    # Start the browser with the custom options
    driver = webdriver.Chrome(options=chrome_options)
    if snap.metadata.source is not None:
        # Only access "url" if snap.metadata.source is not None
        driver.get(snap.metadata.source.url)
    else:
        # Handle the case when snap.metadata.source is None
        print("snap.metadata.source is None, cannot access 'url'.")
    # driver.get(snap.metadata.source.url)

    try:
        # Step 1: Navigate to the section with the specific title
        table_5_section = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//h3[contains(text(), 'Table 5:  Threatened species in each major group by country')]")
            )
        )
        table_5_section.click()  # Click to ensure it's expanded, if necessary

        # Wait for any animations or content loading
        time.sleep(2)

        # Step 2: Within this section, click on the "SHOW ALL" link
        show_all_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//h3[contains(text(), 'Table 5:  Threatened species in each major group by country')]/following-sibling::div//a[@role='link' and contains(text(), 'SHOW ALL')]",
                )
            )
        )
        show_all_link.click()

        # Wait up to 10 seconds until the button is present
        download_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "buttons-csv"))
        )
        download_button.click()
        # Optionally: Wait for a bit to ensure the download starts
        time.sleep(5)
    finally:
        driver.quit()
    # Find the original downaloaded file name
    downloaded_file_name = os.listdir(os.path.dirname(snap.path))[0]

    rename_downloaded_file(os.path.dirname(snap.path), downloaded_file_name, "iucn_animal.csv")

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def rename_downloaded_file(directory_path, original_filename, new_filename):
    """
    Rename the downloaded file in the specified directory.

    Parameters:
    - directory_path (str): The path to the directory where the file was downloaded.
    - original_filename (str): The original name of the downloaded file.
    - new_filename (str): The new name for the file.

    Returns:
    - bool: True if renaming was successful, False otherwise.
    """
    original_filepath = os.path.join(directory_path, original_filename)
    new_filepath = os.path.join(directory_path, new_filename)

    # Check if the original file exists before attempting to rename
    if os.path.exists(original_filepath):
        shutil.move(original_filepath, new_filepath)
        return True
    return False


if __name__ == "__main__":
    main()

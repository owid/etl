"""Script to create a snapshot of dataset."""

import time
from pathlib import Path

import click
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
URL_MAIN = "https://aerospace.csis.org/data/international-astronaut-database/"


def fetch_data(snap_path: Path, snap_url: str) -> None:
    # Ensure the parent directory exists.
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    # Set up Selenium WebDriver with custom download directory.
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    # Set Chrome's download preferences.
    prefs = {
        "download.default_directory": str(snap_path.parent),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    # Initialize the WebDriver and open the webpage.
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(snap_url)
    # Wait for the page to load.
    time.sleep(3)
    # Find the CSV button and click it.
    csv_button = driver.find_element(By.CLASS_NAME, "DTTT_button_csv")
    csv_button.click()
    # Wait for the download to complete
    time.sleep(5)
    # Get the latest downloaded file in the directory.
    latest_file = max(list(snap_path.parent.glob("*.csv")), key=lambda f: f.stat().st_mtime)
    # Rename it to the expected file name.
    latest_file.rename(snap_path)
    # Close the browser.
    driver.quit()


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"space/{SNAPSHOT_VERSION}/international_astronaut_database.csv")

    # Download the data.
    fetch_data(snap_path=snap.path, snap_url=snap.metadata.origin.url_main)

    # Create the snapshot.
    snap.create_snapshot(upload=upload, filename=snap.path)


if __name__ == "__main__":
    main()

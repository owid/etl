"""Script to create a snapshot of dataset."""

import os
import shutil
import time
from pathlib import Path

import click
import owid.catalog.processing as pr
import structlog
from owid.datautils.io import df_to_file
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm

from etl.snapshot import Snapshot

log = structlog.get_logger()


# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cancer/{SNAPSHOT_VERSION}/gco_cancer_survival.csv")
    download_dir = os.path.dirname(snap.path)

    # Create a new snapshot.
    # download_data(download_dir, log)
    df = process_data(download_dir)
    df_to_file(df, file_path=snap.path)

    snap.dvc_add(upload=upload)


def download_data(download_dir, log):
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
        },
    )
    driver = webdriver.Chrome(options=chrome_options)

    cancer_sites = ["Liver", "Colon", "Colorectal", "Lung", "Oesophagus", "Pancreas", "Stomach", "Ovary", "Rectum"]
    countries = ["Norway", "Australia", "Canada", "Denmark", "Ireland", "New Zealand", "United Kingdom"]
    genders = ["All", "Males", "Females"]
    survival_years = [1, 3, 5]

    total_iterations = len(cancer_sites) * len(countries) * len(survival_years) * len(genders)

    with tqdm(total=total_iterations, desc="Processing Downloads", unit="file") as pbar:
        for site in cancer_sites:
            for country in countries:
                gender_list = ["Females"] if site == "Ovary" else genders  # Handle special case for Ovary
                for gender in gender_list:
                    for survival_year in survival_years:
                        url = generate_cancer_url(site, country, gender, survival_year)
                        log.info(f"Navigating to {url}")
                        driver.get(url)
                        try:
                            WebDriverWait(driver, 30).until(
                                EC.visibility_of_element_located(
                                    (By.XPATH, "/html/body/div[4]/ul/li[1]/ul/li[2]/a/span[1]")
                                )
                            )

                            element = WebDriverWait(driver, 20).until(
                                EC.element_to_be_clickable((By.XPATH, "/html/body/div[4]/ul/li[2]/ul/li[1]/a"))
                            )
                            driver.execute_script("arguments[0].scrollIntoView();", element)
                            element.click()

                            log.info(f"Download initiated for {country}, {site}, {gender}, {survival_year}")

                            # Wait for the download to complete
                            if wait_for_download_completion(download_dir):
                                original_file = os.path.join(download_dir, "dataset.csv")
                                new_filename = f"{country}_{site}_{survival_year}_{gender}.csv"
                                new_file_path = os.path.join(download_dir, new_filename)
                                shutil.move(original_file, new_file_path)
                                log.info(f"File renamed to {new_filename}")
                            else:
                                log.info("Download did not complete within the timeout period.")

                            pbar.update(1)

                        except Exception as e:
                            log.info(f"Error during download or file handling: {str(e)}")

    driver.quit()


def wait_for_download_completion(download_dir, filename="dataset.csv", timeout=60):
    """
    Wait for a file to appear in download_dir and ensure it's not still being written to.
    """
    file_path = os.path.join(download_dir, filename)
    end_time = time.time() + timeout
    while time.time() < end_time:
        # Check if the main file exists and no temporary .crdownload file exists
        if os.path.exists(file_path) and not any(fname.endswith(".crdownload") for fname in os.listdir(download_dir)):
            return True
        time.sleep(1)
    return False


def process_data(download_dir):
    dataframes = []
    for filename in os.listdir(download_dir):
        if filename.endswith(".csv"):
            # Remove the file extension
            name_without_ext = filename.replace(".csv", "")

            # Split by underscores
            parts = name_without_ext.split("_")
            country = parts[0]
            cancer = parts[1]
            survival_year = parts[2]
            gender = parts[3].replace(".csv", "")

            file_path = os.path.join(download_dir, filename)
            df = pr.read_csv(file_path, encoding="ISO-8859-1")

            df["country"] = country
            df["cancer"] = cancer
            df["survival_year"] = survival_year
            df["gender"] = gender

            dataframes.append(df)
    all_dfs = pr.concat(dataframes, ignore_index=True)
    return all_dfs


def generate_cancer_url(cancer_site, country, gender, survival_year):
    base_url = "https://gco.iarc.fr/survival/survmark/visualizations/viz2/"
    url = (
        f'{base_url}?cancer_site="{cancer_site}"&country="{country}"'
        f'&agegroup="All"&gender="{gender}"&interval="1"'
        f'&survival_year="{survival_year}"&measures=%5B%22Incidence+%28ASR%29%22%2C%22Mortality+%28ASR%29%22%2C%22Net+Survival%22%5D'
    )
    return url


if __name__ == "__main__":
    main()

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
    snap = Snapshot(f"cancer/{SNAPSHOT_VERSION}/gco_alcohol.csv")
    base_url = snap.metadata.origin.url_download
    download_dir = os.path.dirname(snap.path)

    download_data(base_url, download_dir, log)
    df = process_data(download_dir)
    df_to_file(df, file_path=snap.path)

    snap.dvc_add(upload=upload)


def download_data(base_url, download_dir, log):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    driver = webdriver.Chrome(options=chrome_options)

    sexes = {0: "both", 1: "male", 2: "female"}
    keys = ["paf", "asr"]
    cancers = {
        40: "all",
        20: "breast",
        8: "colon",
        42: "colorectum",
        14: "larynx",
        11: "liver",
        6: "oesophagus",
        1: "oral",
        41: "pharynx",
        9: "rectum",
    }
    fixed_params = "mode=1&population=1&continent=0&age_group=3"
    total_iterations = len(sexes) * len(keys) * len(cancers)

    with tqdm(total=total_iterations, desc="Processing Downloads", unit="file") as pbar:
        for sex_code, sex_str in sexes.items():
            for key in keys:
                for cancer_code, cancer_str in cancers.items():
                    url = f"{base_url}?{fixed_params}&sex={sex_code}&cancer={cancer_code}&key={key}"
                    driver.get(url)
                    WebDriverWait(driver, 20).until(
                        EC.visibility_of_element_located(
                            (By.XPATH, "/html/body/div[2]/div[2]/div/div/div[2]/div[4]/table/tbody/tr[1]/td[2]")
                        )
                    )
                    try:
                        element = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable(
                                (By.XPATH, "/html/body/div[2]/div[2]/div/div/div[2]/div[2]/ul/li[5]/a")
                            )
                        )
                        driver.execute_script("arguments[0].scrollIntoView();", element)
                        element.click()
                        log.info(f"Download initiated for {sex_str}, {cancer_str}, {key}")
                        time.sleep(10)  # Wait for the download to complete
                        original_file = os.path.join(download_dir, "data.csv")
                        new_filename = f"{sex_str}_{cancer_str}_{key}.csv"
                        new_file_path = os.path.join(download_dir, new_filename)
                        if os.path.exists(original_file):
                            shutil.move(original_file, new_file_path)
                            log.info(f"File renamed to {new_filename}")
                        else:
                            log.info("Original file not found. Check download directory and filename.")
                        pbar.update(1)
                    except Exception as e:
                        log.info("Error during download or file handling:", str(e))
    driver.quit()


def process_data(download_dir):
    dataframes = []
    for filename in os.listdir(download_dir):
        if filename.endswith(".csv"):
            parts = filename.split("_")
            sex = parts[0]
            cancer = parts[1]
            key = parts[2].replace(".csv", "")

            file_path = os.path.join(download_dir, filename)
            df = pr.read_csv(file_path, encoding="ISO-8859-1")
            df = df.rename(columns={"Rank": "country", "Country": "value"})

            for column in df.columns:
                if column not in ["value", "country"]:
                    df = df.drop(column, axis=1)

            df["sex"] = sex
            df["cancer"] = cancer
            df["indicator"] = key
            df["year"] = 2020

            dataframes.append(df)
    all_dfs = pr.concat(dataframes, ignore_index=True)
    return all_dfs


if __name__ == "__main__":
    main()

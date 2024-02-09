"""Run as `python un_wpp.py`"""


import glob
import os
import shutil
import tempfile
import time
import zipfile
from pathlib import Path

import requests
from owid.walden import add_to_catalog
from owid.walden.catalog import Dataset
from structlog import get_logger

log = get_logger()


URLS = {
    "fertility": [
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_Fertility_by_Age5.zip",
    ],
    "demographics": [
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_Demographic_Indicators_Medium.zip",
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_Demographic_Indicators_OtherVariants.zip",
    ],
    "population": [
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_PopulationBySingleAgeSex_Medium_1950-2021.zip",
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_PopulationBySingleAgeSex_Medium_2022-2100.zip",
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_PopulationBySingleAgeSex_High_2022-2100.zip",
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_PopulationBySingleAgeSex_Low_2022-2100.zip",
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_PopulationBySingleAgeSex_Constant%20fertility_2022-2100.zip",
    ],
    "deaths": [
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/EXCEL_FILES/4_Mortality/WPP2022_MORT_F01_1_DEATHS_SINGLE_AGE_BOTH_SEXES.xlsx",
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/EXCEL_FILES/4_Mortality/WPP2022_MORT_F01_2_DEATHS_SINGLE_AGE_MALE.xlsx",
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/EXCEL_FILES/4_Mortality/WPP2022_MORT_F01_3_DEATHS_SINGLE_AGE_FEMALE.xlsx",
    ],
    "dependency_ratio": [
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/EXCEL_FILES/2_Population/WPP2022_POP_F07_1_DEPENDENCY_RATIOS_BOTH_SEXES.xlsx",
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/EXCEL_FILES/2_Population/WPP2022_POP_F07_2_DEPENDENCY_RATIOS_MALE.xlsx",
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/EXCEL_FILES/2_Population/WPP2022_POP_F07_3_DEPENDENCY_RATIOS_FEMALE.xlsx",
    ],
}


def download_data(output_dir):
    """Download all data."""
    log.info("Downloading data...")
    for category, urls in URLS.items():
        t0 = time.time()
        log.info(category)
        for url in urls:
            filename = os.path.basename(url)
            log.info(f"\t {filename}")
            output_path = os.path.join(output_dir, filename)
            _download_file(url, output_path)
        t = time.time() - t0
        log.info(f"{t} seconds")
        log.info("---")


def unzip_data(output_dir):
    """Unzip downloaded files (only compressed files)."""
    log.info("Unzipping data...")
    files = [os.path.join(output_dir, f) for f in os.listdir(output_dir)]
    for f in files:
        log.info(f)
        if f.endswith(".zip"):
            _unzip_file(f)


def _download_file(url, output_path):
    """Download individual file."""
    response = requests.get(url, stream=True)
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024 * 10):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def _unzip_file(f):
    """Unzip individual file."""
    output_dir = os.path.dirname(f)
    z = zipfile.ZipFile(f)
    z.extractall(output_dir)


def clean_directory(directory):
    """Remove all zip files.

    This should be applied after uncompressing files.
    """
    log.info("Removing zipped data...")
    files = glob.glob(os.path.join(directory, "*.zip"))
    for f in files:
        os.remove(f)


def compress_directory(directory, output_zip):
    """Compress directory."""
    log.info("Zipping data...")
    shutil.make_archive("un_wpp", "zip", directory)
    return f"{output_zip}.zip"


def prepare_data(directory):
    """Download, unzip, clean and compress all data files.

    Accesses UN WPP data portal, downloads all necessary files (see `URLS`), and creates a zip folder with all of them
    named 'un_wpp.zip'
    """
    output_zip = "un_wpp"
    download_data(directory)
    unzip_data(directory)
    clean_directory(directory)
    output_file = compress_directory(directory, output_zip)
    return output_file


def prepare_metadata():
    log.info("Preparing metadata...")
    path = Path(__file__).parent / f"{Path(__file__).stem}.meta.yml"
    return Dataset.from_yaml(path)


def main():
    with tempfile.TemporaryDirectory() as tmp_dir:
        metadata = prepare_metadata()
        output_file = prepare_data(tmp_dir)
        add_to_catalog(metadata, output_file, upload=True, public=True)


if __name__ == "__main__":
    main()

"""WARNING: This code was imported from walden. So, when executed, it will not create any snapshot.
Next time this data needs to be updated, the code below needs to be adapted to create a snapshot.

Command:

    FILE_DIR=''
    python snapshots/un/2024-07-11/un_wpp.py \
        --deaths-age-fem $FILE_DIR/deaths_age_female.xlsx \
        --deaths-age-male $FILE_DIR/deaths_age_male.xlsx \
        --death-rate $FILE_DIR/death_rate.xlsx \
        --population $FILE_DIR/population.csv \
        --growth-rate $FILE_DIR/grrate.xlsx \
        --nat-change-rate $FILE_DIR/natchange.xlsx \
        --fertility-tot $FILE_DIR/frtot.xlsx \
        --fertility-age $FILE_DIR/frage.xlsx \
        --migration $FILE_DIR/mig.xlsx \
        --migration-rate $FILE_DIR/migrate.xlsx \
        --deaths $FILE_DIR/deaths.xlsx \
        --deaths-age $FILE_DIR/deaths_age.xlsx \
        --births-sex $FILE_DIR/births_sex.xlsx \
        --births-age $FILE_DIR/births_age.xlsx \
        --median-age $FILE_DIR/median_age.xlsx \
        --le $FILE_DIR/le.xlsx \
        --le-f $FILE_DIR/le_f.xlsx \
        --le-m $FILE_DIR/le_m.xlsx


Files needed:

    - Population
        Title (page):           WPP2024_Population_1_July_by_Age1_long.csv.gz
        Filename:               Population by Age July 1 2024.csv
        Alias:                  population.csv
    - Natural change
        Title (page):           WPP2024_Rate_of_natural_change_%28per_thousand%29.xlsx
        Filename:               WPP2024 Natural Change Rate.xlsx
        Alias                   natchange.xlsx
    - Growth rate
        Title (page):           WPP2024_Average_annual_rate_of_population_change-growth_rate_%28%25%29.xlsx
        Filename:               Population Growth Rate 2024.xlsx
        Alias                   grrate.xlsx
    - Fertility (total)
        Title (page):           WPP2024_Total_fertility_rate_%28live_births_per_woman%29.xlsx
        Filename:               WPP2024 Total Fertility Rate.xlsx
        Alias:                  frtot.xlsx
    - Fertility (asfr)
        Title (page):           WPP2024_Age-specific_fertility_rates_%28ASFR%3B_births_per_1%2C000_women%29_Abridged_Ages.xlsx
        Filename:               ASFR Abridged Ages.xlsx
        Alias:                  frage.xlsx
    - Migration (number)
        Title (page):           WPP2024_Net_number_of_migrants_by_sex_%28in_thousands%29.xlsx
        Filename:               Net migrants by sex 2024.xlsx
        Alias:                  mig.xlsx
    - Migration rate
        Title (page):           WPP2024_Crude_net_migration_rate_%28CNMR%29_%28per_1%2C000_population%29.xlsx
        Filename:               WPP2024 Crude Net Migration Rate.xlsx
        Alias:                  migrate.xlsx
    - Deaths (total)
        Title (page):           https://www.dropbox.com/scl/fo/m5ubnjq0j0542px0vowpu/AJbGz8qywO0ZtsrIt4HmVJA/WPP2024_Total_deaths_by_sex_%28in_thousands%29.xlsx?rlkey=jgttagxx80mosd66yi4o61wjh&dl=0
        Filename:               WPP2024 Total Deaths by Sex.xlsx
        Alias:                  deaths.xlsx
    - Deaths (age)
        Title (page):           WPP2024_Deaths_by_age_and_sex_%28in_thousands%29_Abridged_Ages_Total.xlsx
        Filename:               WPP2024 Deaths by Age.xlsx
        Alias:                  deaths_age.xlsx
    - Deaths (age, fem)
        Title (page):           WPP2024_Deaths_by_age_and_sex_%28in_thousands%29_Abridged_Ages_Female.xlsx
        Filename:               WPP2024 Deaths by Age Female.xlsx
        Alias:                  deaths_age_female.xlsx
    - Deaths (age, mal)
        Title (page):           WPP2024_Deaths_by_age_and_sex_%28in_thousands%29_Abridged_Ages_Male.xlsx
        Filename:               WPP2024 Deaths by Age and Sex Male.xlsx
        Alias:                  deaths_age_male.xlsx
    - Death rate
        Title (page):           WPP2024_Crude_death_rate_%28CDR%29_%28deaths_per_1%2C000_population%29.xlsx
        Filename:               WPP2024 Crude Death Rate.xlsx
        Alias:                  death_rate.xlsx
    - Births (sex)
        Title (page):           WPP2024_Total_births_by_sex_%28in_thousands%29.xlsx
        Filename:               WPP2024 Total Births by Sex.xlsx
        Alias:                  births_sex.xlsx
    - Births (age)
        Title (page):           WPP2024_Births_by_age_group_of_mother_%28in_thousands%29_Abridged_Ages.xlsx
        Filename:               Births by Mother Age.xlsx
        Alias:                  births_age.xlsx
    - Median age
        Title (page):           WPP2024_Median_age_of_population_%28years%29.xlsx
        Filename:               Median Age of Population 2024
        Alias:                  median_age.xlsx
    - Life expectancy (total)
        Title (page):           WPP2024_Life_expectancy_at_exact_ages_%28ex%29_in_years_Abridged_Ages_Total.xlsx
        Filename:               Life Expectancy Indicators.xlsx
        Alias:                  le.xlsx
    - Life expectancy (fem)
        Title (page):           WPP2024_Life_expectancy_at_exact_ages_%28ex%29_in_years_Abridged_Ages_Female.xlsx
        Filename:               Life expectancy female.xlsx
        Alias:                  le_fem.xlsx
    - Life expectancy (male)
        Title (page):           WPP2024_Life_expectancy_at_exact_ages_%28ex%29_in_years_Abridged_Ages_Male.xlsx
        Filename:               Life Expectancy Male.xlsx
        Alias:                  le_male.xlsx

"""

import glob
import os
import shutil
import tempfile
import time
import zipfile
from pathlib import Path

import click
import requests
from owid.walden import add_to_catalog
from owid.walden.catalog import Dataset
from structlog import get_logger

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Log
log = get_logger()


########################################################################################################################
# TODO: Temporarily using a local file until 2024 revision is released
#  The download url should still be the same:
#  https://population.un.org/wpp
########################################################################################################################
@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--population", type=str, help="Path to population local file.")
@click.option("--growth-rate", type=str, help="Path to population growth rate local file.")
@click.option("--nat-change-rate", type=str, help="Path to rate natural change local file.")
@click.option("--fertility-tot", type=str, help="Path to total fertility rate local file.")
@click.option("--fertility-age", type=str, help="Path to age-specific fetility rate local file.")
@click.option("--migration", type=str, help="Path to net-migration local file.")
@click.option("--migration-rate", type=str, help="Path to net-migration rate local file.")
@click.option("--deaths", type=str, help="Path to total deaths local file.")
@click.option("--deaths-age", type=str, help="Path to total deaths by age group local file.")
@click.option("--deaths-age-f", type=str, help="Path to female deaths by age group local file.")
@click.option("--deaths-age-m", type=str, help="Path to male deaths by age group local file.")
@click.option("--death-rate", type=str, help="Path to crude death rate local file.")
@click.option("--births-sex", type=str, help="Path to births by sex.")
@click.option("--births-age", type=str, help="Path to births by age group of the mother.")
@click.option("--median-age", type=str, help="Path to median age of the population.")
@click.option("--le", type=str, help="Path to median age of the population.")
@click.option("--le-f", type=str, help="Path to median age of the population.")
@click.option("--le-m", type=str, help="Path to median age of the population.")
def main(
    upload: bool,
    population: str | None = None,
    growth_rate: str | None = None,
    nat_change_rate: str | None = None,
    fertility_tot: str | None = None,
    fertility_age: str | None = None,
    migration: str | None = None,
    migration_rate: str | None = None,
    deaths: str | None = None,
    deaths_age: str | None = None,
    deaths_age_f: str | None = None,
    deaths_age_m: str | None = None,
    death_rate: str | None = None,
    births_sex: str | None = None,
    births_age: str | None = None,
    median_age: str | None = None,
    le: str | None = None,
    le_f: str | None = None,
    le_m: str | None = None,
) -> None:
    snapshot_paths = [
        (population, "un_wpp_population.csv"),
        (growth_rate, "un_wpp_growth_rate.xlsx"),
        (nat_change_rate, "un_wpp_nat_change_rate.xlsx"),
        (fertility_tot, "un_wpp_fert_rate_tot.xlsx"),
        (fertility_age, "un_wpp_fert_rate_age.xlsx"),
        (migration, "un_wpp_migration.xlsx"),
        (migration_rate, "un_wpp_migration_rate.xlsx"),
        (deaths, "un_wpp_deaths.xlsx"),
        (deaths_age, "un_wpp_deaths_age.xlsx"),
        (deaths_age_f, "un_wpp_deaths_age_fem.xlsx"),
        (deaths_age_m, "un_wpp_deaths_age_male.xlsx"),
        (death_rate, "un_wpp_death_rate.xlsx"),
        (births_sex, "un_wpp_births_sex.xlsx"),
        (births_age, "un_wpp_births_age.xlsx"),
        (le, "un_wpp_le.xlsx"),
        (le_f, "un_wpp_le_f.xlsx"),
        (le_m, "un_wpp_le_m.xlsx"),
    ]
    for paths in snapshot_paths:
        if paths[0] is not None:
            log.info(f"Importing {paths[1]}.")
            # Create a new snapshot.
            snap = Snapshot(f"un/{SNAPSHOT_VERSION}/{paths[1]}")

            # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
            snap.create_snapshot(filename=paths[0], upload=upload)
        else:
            log.warning(f"Skipping import for {paths[1]}.")


########################################################################################################################
########################################################################################################################
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


def main_tmp():
    with tempfile.TemporaryDirectory() as tmp_dir:
        metadata = prepare_metadata()
        output_file = prepare_data(tmp_dir)
        add_to_catalog(metadata, output_file, upload=True, public=True)


if __name__ == "__main__":
    main()

"""Script to create a snapshot of dataset 'WHO Mortality Database (2022)'.
The data download for this step is quite manual.

Go to:
https://platform.who.int/mortality

Click on each of the causes in Malignant Neoplasms e.g. Breast Cancer

Click on each of the levels below e.g. Mesothelioma

Click the download button in the top right and select 'Full Dataset'

Do this for each of the top-level categories found in 'list_of_causes' below, and All Causes.

Save them in the same folder and read from this folder when creating the snapshot.

ICD 10 codes were gathered from the downloaded files and the broad cause group was inferred from the platform.

"""
import os
from pathlib import Path

import click
import pandas as pd
from structlog import get_logger

from etl.snapshot import Snapshot, add_snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
LOCAL_DIR = "/Users/fionaspooner/Desktop/who-mdb-cancer/"
log = get_logger()


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/mortality_database_cancer.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    df = combine_datasets()
    add_snapshot(f"who/{SNAPSHOT_VERSION}/mortality_database_cancer.csv", dataframe=df, upload=upload)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def combine_datasets() -> pd.DataFrame:
    base_file_path = LOCAL_DIR
    list_of_files = os.listdir(base_file_path)
    csv_files = list(filter(lambda f: f.endswith(".csv"), list_of_files))
    list_of_causes = {
        "Mouth and oropharynx cancers": {"icd_codes": "C00-C14"},
        "Oesophagus cancer": {"icd_codes": "C15"},
        "Stomach cancer": {"icd_codes": "C16"},
        "Colon and rectum cancers": {"icd_codes": "C18-C21"},
        "Liver cancer": {"icd_codes": "C22"},
        "Pancreas cancer": {"icd_codes": "C25"},
        "Trachea, bronchus, lung cancers": {"icd_codes": "C33-C34"},
        "Melanoma and other skin cancers": {"icd_codes": "C43-C44"},
        "Breast cancer": {"icd_codes": "C50"},
        "Cervix uteri cancer": {"icd_codes": "C53"},
        "Corpus uteri cancer": {"icd_codes": "C54-C55"},
        "Ovary cancer": {"icd_codes": "C56"},
        "Prostate cancer": {"icd_codes": "C61"},
        "Testicular cancer": {"icd_codes": "C62"},
        "Kidney cancer": {"icd_codes": "C64-C66"},
        "Bladder cancer": {"icd_codes": "C67"},
        "Brain and nervous system cancers": {"icd_codes": "C70-C72"},
        "Gallbladder and biliary tract cancer": {"icd_codes": "C23-C24"},
        "Larynx cancer": {"icd_codes": "C32"},
        "Thyroid cancer": {"icd_codes": "C73"},
        "Mesothelioma": {"icd_codes": "C45"},
        "Lymphomas, multiple myeloma": {"icd_codes": "C81-C90, C96"},
        "Leukaemia": {"icd_codes": "C91-C95"},
        "Ill-defined malignant neoplasms": {"icd_codes": "C76, C80, C97"},
        "Other malignant neoplasms": {
            "icd_codes": "C17, C26-C31, C37-C41, C46-C49, C51,C52, C57-C60, C63, C68, C69, C74-C75, C77- C79"
        },
    }
    df_all = pd.DataFrame()
    for cause in list_of_causes.keys():
        log.info(f"Processing {cause}")
        file_name = [s for s in csv_files if cause in s]
        file_name = "".join(file_name)
        file_path = base_file_path + file_name
        assert file_path.endswith("csv")
        df = pd.read_csv(file_path, skiprows=6)
        cols = df.columns
        df = df.reset_index()
        df = df.dropna(how="all", axis=1)
        df.columns = cols
        df["cause"] = cause
        df["icd10_codes"] = list_of_causes[cause]["icd_codes"]
        df_all = pd.concat([df_all, df])
    return df_all.reset_index(drop=True)


if __name__ == "__main__":
    main()

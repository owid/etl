"""Script to create a snapshot of dataset 'WHO Mortality Database (2022)'.
The data download for this step is quite manual.

Go to:
https://platform.who.int/mortality

Click on Communicable, maternal, perinatal and nutritional conditions

Click on each of the diseases in the LIST_OF_CAUSES below

Click the download button in the top right and select 'Full Dataset'

Save them in the same folder and read from this folder when creating the snapshot.

ICD 10 codes were gathered from the downloaded files and the broad cause group was inferred from the platform.

"""

import os
from pathlib import Path
from typing import Dict

import click
import pandas as pd
from structlog import get_logger

from etl.snapshot import Snapshot

LIST_OF_CAUSES = {
    "Pertussis": {"icd_codes": "A37"},
    "Poliomyelitis": {"icd_codes": "A80, B91, G14"},
    "Diphtheria": {"icd_codes": "A36"},
    "Measles": {"icd_codes": "B05"},
    "Tetanus": {"icd_codes": "A33-A35"},
    "Hepatitis B": {"icd_codes": "B16-B19 (minus B17.1, B18.2)"},
    "Japanese encephalitis": {"icd_codes": "A83"},
}
# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
LOCAL_DIR = "/Users/fionaspooner/Desktop/who-mdb/"
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
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/mortality_database_vaccine_preventable.csv")

    df = combine_datasets(LIST_OF_CAUSES)

    snap.create_snapshot(data=df, upload=upload)


def combine_datasets(list_of_causes: Dict) -> pd.DataFrame:
    base_file_path = LOCAL_DIR
    list_of_files = os.listdir(base_file_path)
    csv_files = list(filter(lambda f: f.endswith(".csv"), list_of_files))

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

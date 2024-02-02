"""Script to create a snapshot of dataset 'WHO Mortality Database (2022)'.
The data download for this step is quite manual.

Go to:
https://platform.who.int/mortality

Click on each of the top-level causes e.g. Injuries.

Click on each of the levels below e.g. Unintentional injuries.

Click the download button in the top right and select 'Full Dataset'

Do this for each of the top-level categories found in 'list_of_causes' below, and All Causes.

Save them in the same folder and read from this folder when creating the snapshot.

ICD 10 codes were gathered from the downloaded files and the broad cause group was inferred from the platform.

"""
import os
from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot, add_snapshot

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
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/mortality_database.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    df = combine_datasets()
    add_snapshot("who/2023-08-01/mortality_database.csv", dataframe=df, upload=upload)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def combine_datasets() -> pd.DataFrame:
    base_file_path = "/Users/fionaspooner/Documents/OWID/repos/etl/snapshots/who/2023-08-01/who_mort_db/"
    list_of_files = os.listdir(base_file_path)
    csv_files = list(filter(lambda f: f.endswith(".csv"), list_of_files))
    list_of_causes = {
        "All Causes": {"icd_codes": "A00-Y89", "broad_cause_group": "All Causes"},
        "Cardiovascular diseases": {"icd_codes": "I00-I99", "broad_cause_group": "Noncommunicable diseases"},
        "Congenital anomalies": {"icd_codes": "Q00-Q99", "broad_cause_group": "Noncommunicable diseases"},
        "Diabetes mellitus and endocrine disorders": {
            "icd_codes": "E10-E14, D55-D64 (minus D64.9),D65-D89, E03-E07, E15-E16, E20-E34, E65-E88",
            "broad_cause_group": "Noncommunicable diseases",
        },
        "Digestive diseases": {"icd_codes": "K20-K92", "broad_cause_group": "Noncommunicable diseases"},
        "Ill-defined injuries": {"icd_codes": "Y10-Y34, Y872", "broad_cause_group": "Injuries"},
        "Infectious and parasitic diseases": {
            "icd_codes": "A00-B99, G00-G04, G14, N70-N73, P37.3, P37.4",
            "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
        },
        "Intentional injuries": {"icd_codes": "X60-Y09, Y35-Y36, Y870, Y871", "broad_cause_group": "Injuries"},
        "Malignant neoplasms": {"icd_codes": "C00-C97", "broad_cause_group": "Noncommunicable diseases"},
        "Maternal conditions": {
            "icd_codes": "O00-O99",
            "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
        },
        "Musculoskeletal diseases": {"icd_codes": "M00-M99", "broad_cause_group": "Noncommunicable diseases"},
        "Neuropsychiatric conditions": {
            "icd_codes": "F01-F99, G06-G98 (minus G14), U07.0, X41, X42, X44, X45",
            "broad_cause_group": "Noncommunicable diseases",
        },
        "Nutritional deficiencies": {
            "icd_codes": "E00-E02, E40-E46, E50, D50-D53,D64.9, E51-E64",
            "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
        },
        "Oral conditions": {"icd_codes": "K00-K14", "broad_cause_group": "Noncommunicable diseases"},
        "Other neoplasms": {"icd_codes": "D00-D48", "broad_cause_group": "Noncommunicable diseases"},
        "Perinatal conditions": {
            "icd_codes": "P00-P96 (minus P23, P37.3, P37.4)",
            "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
        },
        "Respiratory diseases": {"icd_codes": "J30-J98", "broad_cause_group": "Noncommunicable diseases"},
        "Respiratory infections": {
            "icd_codes": "H65-H66, J00-J22,  P23, U04, U07.1, U07.2, U09.9, U10.9",
            "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
        },
        "Sense organ diseases": {"icd_codes": "H00-H61, H68-H93", "broad_cause_group": "Noncommunicable diseases"},
        "Skin diseases": {"icd_codes": "L00-L98", "broad_cause_group": "Noncommunicable diseases"},
        "Sudden infant death syndrome": {"icd_codes": "R95", "broad_cause_group": "Noncommunicable diseases"},
        "Unintentional injuries": {
            "icd_codes": "V01-X59, Y40-Y86, Y88, Y89 (minus X41-X42, X44-X45), U12.9",
            "broad_cause_group": "Injuries",
        },
        "Ill-defined diseases": {"icd_codes": "R00-R94, R96-R99", "broad_cause_group": "Ill-defined diseases"},
    }
    df_all = pd.DataFrame()
    for cause in list_of_causes.keys():
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
        df["broad_cause_group"] = list_of_causes[cause]["broad_cause_group"]
        df_all = pd.concat([df_all, df])
    return df_all.reset_index(drop=True)


if __name__ == "__main__":
    main()

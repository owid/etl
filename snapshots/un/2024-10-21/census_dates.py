"""Script to create a snapshot of dataset.
This script creates a snapshot from a local file.
The local file is created by copy-pasting the data table from this website: https://unstats.un.org/unsd/demographic-social/census/censusdates/ and then running the "clean_file" function"""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


# These are helper functions to clean the data from the website
def file_to_list(filename):
    with open(filename) as f:
        return [line.strip() for line in f]


def clean_list_entries(txt_ls):
    rm_entries = [
        "AFRICA",
        "ASIA",
        "EUROPE",
        "AMERICA, SOUTH",
        "AMERICA, NORTH",
        "OCEANIA",
        "Countries or areas",
        "1990 round",
        "2000 round",
        "2010 round",
        "2020 round",
        "(1985-1994)",
        "(1995-2004)",
        "(2005-2014)",
        "(2015-2024)",
        "-",
        "",
        "(16) -",
        "(19) -",
        "F",
    ]
    txt_ls = [x for x in txt_ls if x not in rm_entries]

    for i in range(0, len(txt_ls)):
        if txt_ls[i].startswith("(") and txt_ls[i].endswith(")"):
            txt_ls[i] = txt_ls[i][1:-1]

    return txt_ls


def list_to_dict(txt_ls):
    i = 0
    rows = []
    while i < len(txt_ls):
        entry = txt_ls[i]
        last_element = entry.split(" ")
        if not all(char.isdigit() for char in last_element):  # if last element is not a number -> it is a country name
            cty = entry
            i += 1
            if i < len(txt_ls):
                while (all(char.isdigit() for char in txt_ls[i].split(" ")[-1])) or (
                    txt_ls[i] in ["(H) [2003]", "31 Dec.2011-31 Mar.2012", "1985-1989"]
                ):
                    date = txt_ls[i]
                    rows.append({"Country": cty, "Date": date})
                    i += 1
                    if i == len(txt_ls):
                        break
        else:
            print("weird entry: ", entry)
            i += 1

    return rows


def clean_file(filename, output_filename):
    """Clean the result of copy-pasting the website and save it as a csv."""
    txt_ls = file_to_list(filename)
    txt_ls = clean_list_entries(txt_ls)
    census_list = list_to_dict(txt_ls)
    census_df = pd.DataFrame(census_list)
    census_df.to_csv(output_filename, index=False)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/census_dates.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()

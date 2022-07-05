"""This script should be manually adapted and executed on the event of an update of the Maddison Project Database.

"""

import argparse
import pathlib

import sh
import yaml
from owid.walden import files

dvc = sh.Command(".venv/bin/dvc")


CURRENT_DIR = pathlib.Path(__file__).parent


def main() -> None:
    with open(CURRENT_DIR / "ggdc_maddison.meta.yml", "r") as istream:
        meta = yaml.safe_load(istream)

    datafile = str(CURRENT_DIR / "ggdc_maddison.xlsx")
    files.download(meta["source_data_url"], datafile)

    dvc.add(datafile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()

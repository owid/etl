"""Garden step that combines various datasets related to greenhouse emissions and produces the OWID CO2 dataset.

The combined datasets are:
* Global Carbon Budget - Global Carbon Project.
* National contributions to climate change - Jones et al.
* Greenhouse gas emissions by sector - Climate Watch.
* Primary energy consumption - EI & EIA.

Additionally, OWID's regions dataset, population dataset and Maddison Project Database (Bolt and van Zanden, 2023) on
GDP are included.

Outputs:
* The main data file and codebook (both in .csv format) will be committed to the co2-data repository.

"""
import os
import tempfile
from pathlib import Path

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from apps.owidbot import github_utils as gh
from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_and_save_outputs(tb: Table, codebook: Table, temp_dir_path: Path) -> None:
    # Create codebook and save it as a csv file.
    log.info("Creating codebook csv file.")
    pd.DataFrame(codebook).to_csv(temp_dir_path / "owid-co2-codebook.csv", index=False)

    # Create a csv file.
    log.info("Creating csv file.")
    pd.DataFrame(tb).to_csv(temp_dir_path / "owid-co2-data.csv", index=False, float_format="%.3f")


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load the owid_co2 emissions dataset from garden, and read its main table and codebook.
    ds_gcp = paths.load_dataset("owid_co2")
    tb = ds_gcp.read("owid_co2")
    codebook = ds_gcp.read("owid_co2_codebook")

    #
    # Save outputs.
    #
    # If you want to really commit the data, use `CO2_BRANCH=my-branch etlr github/co2_data --export`
    if os.environ.get("CO2_BRANCH"):
        dry_run = False
        branch = os.environ["CO2_BRANCH"]
    else:
        dry_run = True
        branch = "master"

    # Create a temporary directory for all files to be committed.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        prepare_and_save_outputs(tb, codebook=codebook, temp_dir_path=temp_dir_path)

        # TODO: Create README as well.

        # Commit csv files to the repos.
        for file_name in ["owid-co2-data.csv", "owid-co2-codebook.csv"]:
            with (temp_dir_path / file_name).open("r") as file_content:
                gh.commit_file_to_github(
                    file_content.read(),
                    repo_name="co2-data",
                    file_path=file_name,
                    commit_message=":bar_chart: Automated update",
                    branch=branch,
                    dry_run=dry_run,
                )

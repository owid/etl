"""Garden step that combines various datasets related to greenhouse emissions and produces the OWID CO2 dataset.

Datasets combined:
* Global Carbon Budget - Global Carbon Project.
* National contributions to climate change - Jones et al.
* Greenhouse gas emissions by sector - Climate Watch.
* Primary energy consumption - EI & EIA.

Additionally, OWID's regions dataset, population dataset and Maddison Project Database (Bolt and van Zanden, 2023) on
GDP are included.

"""

import os

from apps.owidbot import github_utils as gh
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load the owid_co2 emissions dataset from garden, and read its main table.
    ds_gcp = paths.load_dataset("owid_co2")
    tb = ds_gcp.read("owid_co2", reset_index=False)

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

    gh.commit_file_to_github(
        tb.to_csv(),
        repo_name="co2-data",
        file_path="owid-co2-data.csv",
        commit_message=":bar_chart: Automated update",
        branch=branch,
        dry_run=dry_run,
    )

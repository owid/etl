"""Garden step that combines various datasets related to greenhouse emissions and produces the OWID CO2 dataset.

The combined datasets are:
* Global Carbon Budget - Global Carbon Project.
* National contributions to climate change - Jones et al.
* Primary energy consumption - EI & EIA.

Additionally, OWID's regions dataset, population dataset and Maddison Project Database (Bolt and van Zanden, 2023) on
GDP are included.

Outputs that will be committed to a branch in the co2-data repository:
* The main data file (as a .csv file).
* The codebook (as a .csv file).
* The README file.

"""

import os
import re
import tempfile
from pathlib import Path

import git
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.git_api_helpers import GithubApiRepo
from etl.helpers import PathFinder
from etl.paths import BASE_DIR
from etl.version_tracker import VersionTracker

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_readme() -> str:
    # NOTE: In a future update, we could figure out a way to generate the main content of the README from the table's metadata (possibly with the help of VersionTracker).
    # origins = {origin.title_snapshot or origin.title: origin for origin in set(sum([tb[column].metadata.origins for column in tb.columns], []))}
    df = VersionTracker().steps_df

    # Get all dependencies of the current step.
    dependencies = df[df["step"] == paths.step]["all_active_dependencies"].item()

    # Get the versions of the main steps.
    gcb_version = [step for step in dependencies if "global_carbon_budget" in step if "garden" in step][0].split("/")[
        -2
    ]
    jones_version = [step for step in dependencies if "national_contributions" in step if "garden" in step][0].split(
        "/"
    )[-2]
    owid_co2_version = [step for step in dependencies if "owid_co2" in step if "garden" in step][0].split("/")[-2]

    # Get the versions of the auxiliary steps.
    regions_version = [step for step in dependencies if "regions" in step][0].split("/")[-2]
    population_version = [step for step in dependencies if "population" in step][0].split("/")[-2]
    income_groups_version = [step for step in dependencies if "income_groups" in step][0].split("/")[-2]
    gdp_version = [step for step in dependencies if "maddison" in step][0].split("/")[-2]
    stat_review_version = [step for step in dependencies if "statistical_review" in step][0].split("/")[-2]
    eia_version = [step for step in dependencies if "eia" in step][0].split("/")[-2]
    primary_energy_version = [step for step in dependencies if "primary_energy" in step][0].split("/")[-2]

    readme = f"""\
# Data on CO2 and Greenhouse Gas Emissions by *Our World in Data*

Our complete CO2 and Greenhouse Gas Emissions dataset is a collection of key metrics maintained by [*Our World in Data*](https://ourworldindata.org/co2-and-other-greenhouse-gas-emissions). It is updated regularly and includes data on CO2 emissions (annual, per capita, cumulative and consumption-based), other greenhouse gases, energy mix, and other relevant metrics.

## The complete *Our World in Data* CO2 and Greenhouse Gas Emissions dataset

### 🗂️ Download our complete CO2 and Greenhouse Gas Emissions dataset : [CSV](https://owid-public.owid.io/data/co2/owid-co2-data.csv) | [XLSX](https://owid-public.owid.io/data/co2/owid-co2-data.xlsx) | [JSON](https://owid-public.owid.io/data/co2/owid-co2-data.json)

The CSV and XLSX files follow a format of 1 row per location and year. The JSON version is split by country, with an array of yearly records.

The indicators represent all of our main data related to CO2 emissions, other greenhouse gas emissions, energy mix, as well as other indicators of potential interest.

We will continue to publish updated data on CO2 and Greenhouse Gas Emissions as it becomes available. Most metrics are published on an annual basis.

A [full codebook](https://github.com/owid/co2-data/blob/master/owid-co2-codebook.csv) is made available, with a description and source for each indicator in the dataset. This codebook is also included as an additional sheet in the XLSX file.

## Our source data and code

The dataset is built upon a number of datasets and processing steps:

- Global carbon budget - Global Carbon Project:
  - [Source data](https://globalcarbonbudgetdata.org/)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/gcp/{gcb_version}/global_carbon_budget.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/gcp/{gcb_version}/global_carbon_budget.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/gcp/{gcb_version}/global_carbon_budget.py)
- National contributions to climate change - Jones et al.:
  - [Source data](https://zenodo.org/records/7636699/latest)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/emissions/{jones_version}/national_contributions.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/emissions/{jones_version}/national_contributions.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/emissions/{jones_version}/national_contributions.py)
- Our World in data's CO2 dataset (based on all sources above):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/emissions/{owid_co2_version}/owid_co2.py)
  - [Exporting code](https://github.com/owid/etl/blob/master/etl/steps/export/github/co2_data/latest/owid_co2.py)
  - [Uploading code](https://github.com/owid/etl/blob/master/etl/steps/export/s3/co2_data/latest/owid_co2.py)

Additionally, to construct indicators per capita, per GDP, and per unit energy, we use the following datasets and processing steps:
- Regions (Our World in Data).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/regions/{regions_version}/regions.py)
- Population (Our World in Data based on [a number of different sources](https://ourworldindata.org/population-sources)).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/demography/{population_version}/population/__init__.py)
- Income groups (World Bank).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/wb/{income_groups_version}/income_groups.py)
- GDP (University of Groningen GGDC's Maddison Project Database, Bolt and van Zanden).
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/ggdc/{gdp_version}/maddison_project_database.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/ggdc/{gdp_version}/maddison_project_database.py)
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/ggdc/{gdp_version}/maddison_project_database.py)
- Statistical review of world energy (Energy Institute, EI):
  - [Source data](https://www.energyinst.org/statistical-review)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/energy_institute/{stat_review_version}/statistical_review_of_world_energy.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/energy_institute/{stat_review_version}/statistical_review_of_world_energy.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy_institute/{stat_review_version}/statistical_review_of_world_energy.py)
- International energy data (U.S. Energy Information Administration, EIA):
  - [Source data](https://www.eia.gov/opendata/bulkfiles.php)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/eia/{eia_version}/international_energy_data.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/eia/{eia_version}/energy_consumption.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/eia/{eia_version}/energy_consumption.py)
- Primary energy consumption (EI's Statistical review of world energy & EIA's International energy data):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy/{primary_energy_version}/primary_energy_consumption.py)

## Changelog

- 2025-11-13:
  - Updated dataset to use the latest version of the Global Carbon Budget (2025).
- 2024-11-21:
  - Updated dataset (and codebook) to use the latest version of the Global Carbon Budget (2024), and Jones et al. (2024) (version 2024.2).
  - Now methane, nitrous oxide, and total greenhouse gas emissions data come from Jones et al. (2024), instead of Climate Watch, to provide a wider data coverage.
- 2024-06-20:
  - Update data from the Statistical Review of World Energy.
  - Update data from the Maddison Project Database.
- 2024-04-10:
  - Updated dataset and codebook to use the latest version of the data on National contributions to climate change (Jones et al. (2024)).
- 2023-12-28:
  - Enhanced codebook (improved descriptions, added units, updated sources).
  - Updated primary energy consumption (to update metadata, nothing has changed in the data).
- 2023-12-05:
  - Updated dataset (and codebook) to use the latest version of the Global Carbon Budget (2023).
    - In this version, "International transport" has been replaced by "International aviation" and "International shipping". Also, some overseas territories have no data in this version. More details on the changes can be found in the pdf file hosted [here](https://zenodo.org/records/10177738).
- 2023-11-08:
  - Updated CO2 emissions data to use the latest emissions by sector from Climate Watch (2023).
  - Update codebook accordingly.
- 2023-10-16:
  - Improved codebook.
  - Fixed issue related to consumption-based emissions in Africa, and Palau emissions.
- 2023-07-10:
  - Updated primary energy consumption and other indicators relying on energy data, to use the latest Statistical Review of World Energy by the Energy Institute.
  - Renamed countries 'East Timor' and 'Faroe Islands'.
- 2023-05-04:
  - Added indicators `share_of_temperature_change_from_ghg`, `temperature_change_from_ch4`, `temperature_change_from_co2`, `temperature_change_from_ghg`, and `temperature_change_from_n2o` using data from Jones et al. (2023).
- 2022-11-11:
  - Updated CO2 emissions data with the newly released Global Carbon Budget (2022) by the Global Carbon Project.
  - Added various new indicators related to national land-use change emissions.
  - Added the emissions of the 1991 Kuwaiti oil fires in Kuwait's emissions (while also keeping 'Kuwaiti Oil Fires (GCP)' as a separate entity), to properly account for these emissions in the aggregate of Asia.
  - Applied minor changes to entity names (e.g. "Asia (excl. China & India)" -> "Asia (excl. China and India)").
- 2022-09-06:
  - Updated data on primary energy consumption (from BP & EIA) and greenhouse gas emissions by sector (from CAIT).
  - Refactored code, since now this repository simply loads the data, generates the output files, and uploads them to the cloud; the code to generate the dataset is now in our [etl repository](https://github.com/owid/etl).
  - Minor changes in the codebook.
- 2022-04-15:
  - Updated primary energy consumption data.
  - Updated CO2 data to include aggregations for the different country income levels.
- 2022-02-24:
  - Updated greenhouse gas emissions data from CAIT Climate Data Explorer.
  - Included two new columns in dataset: total greenhouse gases excluding land-use change and forestry, and the same as per capita values.
- 2021-11-05: Updated CO2 emissions data with the newly released Global Carbon Budget (v2021).
- 2021-09-16:
  - Fixed data quality issues in CO2 emissions indicators (emissions less than 0, missing data for Eswatini, ...).
  - Replaced all input CSVs with data retrieved directly from ourworldindata.org.
- 2021-02-08: Updated this dataset with the latest annual release from the Global Carbon Project.
- 2020-08-07: The first version of this dataset was made available.

## Data processing

- **We standardize names of countries and regions.** Since the names of countries and regions are different in different data sources, we standardize all names in order to minimize data loss during data merges.
- **We recalculate carbon emissions to CO2.** The primary data sources on CO2 emissions—the Global Carbon Project, for example—typically report emissions in tonnes of carbon. We have recalculated these figures as tonnes of CO2 using a conversion factor of 3.664.
- **We calculate per capita figures.** All of our per capita figures are calculated from our metric `Population`, which is included in the complete dataset. These population figures are sourced from [Gapminder](http://gapminder.org) and the [UN World Population Prospects (UNWPP)](https://population.un.org/wpp/).

## License

All visualizations, data, and code produced by _Our World in Data_ are completely open access under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce these in any medium, provided the source and authors are credited.

The data produced by third parties and made available by _Our World in Data_ is subject to the license terms from the original third-party authors. We will always indicate the original source of the data in our database, and you should always check the license of any such third-party data before use.

## Authors

This data has been collected, aggregated, and documented by Hannah Ritchie, Max Roser, Edouard Mathieu, Bobbie Macdonald and Pablo Rosado.

The mission of *Our World in Data* is to make data and research on the world's largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).

## How to cite this data?

If you are using this dataset, please cite both [Our World in Data](https://ourworldindata.org/co2-and-greenhouse-gas-emissions#article-citation) and the underlying data source(s).

Please follow [the guidelines in our FAQ](https://ourworldindata.org/faqs#how-should-i-cite-your-data) on how to cite our work.

"""

    log_dates = re.findall("\d{4}-\d{2}-\d{2}", readme.split("Changelog\n")[-1])
    error = "Update the change log to add the latest update."
    assert max(log_dates) >= max([gcb_version, jones_version, owid_co2_version]), error

    return readme


def prepare_and_save_outputs(tb: Table, readme: str, temp_dir_path: Path) -> None:
    # Create codebook from table metadata and save it as a csv file.
    log.info("Saving codebook csv file.")
    tb.codebook.to_csv(temp_dir_path / "owid-co2-codebook.csv", index=False)

    # Create a csv file.
    log.info("Saving data csv file.")
    pd.DataFrame(tb).to_csv(temp_dir_path / "owid-co2-data.csv", index=False, float_format="%.3f")

    # Create a README file.
    log.info("Saving README file.")
    (temp_dir_path / "README.md").write_text(readme)


def run() -> None:
    #
    # Load data.
    #
    # Load the owid_co2 emissions dataset from garden, and read its main table.
    ds_gcp = paths.load_dataset("owid_co2")
    tb = ds_gcp.read("owid_co2")

    #
    # Process data.
    #
    # Create a README file.
    readme = prepare_readme()

    #
    # Save outputs.
    #
    branch = git.Repo(BASE_DIR).active_branch.name

    if branch == "master":
        log.warning("You are on master branch, using dry mode.")
        dry_run = True
    else:
        # Load DRY_RUN from env or use False as default.
        dry_run = bool(int(os.environ.get("DRY_RUN", 0)))
        if dry_run:
            log.info(f"Dry run mode: Would commit files to branch {branch}")
        else:
            log.info(f"Committing files to branch {branch}")

    # Create a temporary directory for all files to be committed.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        prepare_and_save_outputs(tb, readme=readme, temp_dir_path=temp_dir_path)

        repo = GithubApiRepo(repo_name="co2-data")

        repo.create_branch_if_not_exists(branch_name=branch, dry_run=dry_run)

        # Commit csv files to the repos.
        for file_name in ["owid-co2-data.csv", "owid-co2-codebook.csv", "README.md"]:
            with (temp_dir_path / file_name).open("r") as file_content:
                repo.commit_file(
                    file_content.read(),
                    file_path=file_name,
                    commit_message=":bar_chart: Automated update",
                    branch=branch,
                    dry_run=dry_run,
                )

    if not dry_run:
        log.info(
            f"Files committed successfully to branch {branch}. Create a PR here https://github.com/owid/co2-data/compare/master...{branch}."
        )

    # Uncomment to inspect changes (after the new branch has been created).
    # from etl.data_helpers.misc import compare_tables
    # old = pd.read_csv("https://raw.githubusercontent.com/owid/co2-data/refs/heads/master/owid-co2-data.csv")
    # new = pd.read_csv(f"https://raw.githubusercontent.com/owid/co2-data/refs/heads/{branch}/owid-co2-data.csv")
    # compare_tables(old, new, countries=["World"])

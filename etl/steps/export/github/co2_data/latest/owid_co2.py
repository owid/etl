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

import tempfile
from pathlib import Path

import git
import pandas as pd
from owid.catalog import ChangelogEntry, Table
from owid.catalog.core.docs import render_changelog
from structlog import get_logger

from etl import config
from etl.git_api_helpers import GithubApiRepo
from etl.helpers import PathFinder
from etl.paths import BASE_DIR
from etl.version_tracker import VersionTracker

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_readme(changelog: list[ChangelogEntry]) -> str:
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
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/demography/{population_version}/population.py)
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
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/eia/{eia_version}/international_energy.zip.dvc)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/eia/{eia_version}/international_energy.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/eia/{eia_version}/international_energy.py)
- Primary energy consumption (EI's Statistical review of world energy & EIA's International energy data):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy/{primary_energy_version}/primary_energy_consumption.py)

## Changelog

{render_changelog(changelog)}

## Data processing

- **We standardize names of countries and regions.** Since the names of countries and regions are different in different data sources, we standardize all names in order to minimize data loss during data merges.
- **We recalculate carbon emissions to CO2.** The primary data sources on CO2 emissions—the Global Carbon Project, for example—typically report emissions in tonnes of carbon. We have recalculated these figures as tonnes of CO2 using a conversion factor of 3.664.
- **We calculate per capita figures.** All of our per capita figures are calculated from our metric `population`, which is included in the complete dataset.
- **We create aggregate data for regions (e.g. Africa, Europe, etc.).** Since regions are defined differently by our sources, we create our own aggregates following [*Our World in Data* region definitions](https://ourworldindata.org/world-region-map-definitions).
  - We also include data for regions as defined in the original datasets; for example, `Europe (EI)` corresponds to Europe as defined by the Energy Institute.

## License

All visualizations, data, and code produced by _Our World in Data_ are completely open access under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce these in any medium, provided the source and authors are credited.

The data produced by third parties and made available by _Our World in Data_ is subject to the license terms from the original third-party authors. We will always indicate the original source of the data in our database, and you should always check the license of any such third-party data before use.

## Authors

This data has been collected, aggregated, and documented by Pablo Rosado, Hannah Ritchie, Max Roser, Edouard Mathieu, and Bobbie Macdonald.

The mission of *Our World in Data* is to make data and research on the world's largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).

## How to cite this data?

If you are using this dataset, please cite both [Our World in Data](https://ourworldindata.org/co2-and-greenhouse-gas-emissions#article-citation) and the underlying data source(s).

Please follow [the guidelines in our FAQ](https://ourworldindata.org/faqs#how-should-i-cite-your-data) on how to cite our work.

"""

    # The changelog lives in the garden dataset's metadata; it must cover the latest update of its main inputs too.
    error = "Update the changelog in the garden owid_co2.meta.yml to add the latest update."
    assert max((entry.date for entry in changelog), default="") >= max(
        [gcb_version, jones_version, owid_co2_version]
    ), error

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
    readme = prepare_readme(changelog=ds_gcp.metadata.changelog)

    #
    # Save outputs.
    #
    branch = git.Repo(BASE_DIR).active_branch.name

    if branch == "master":
        log.warning("You are on master branch, using dry mode.")
        dry_run = True
    else:
        # Without --export, build the files but don't commit them.
        dry_run = not config.EXPORT_ENABLED
        if dry_run:
            log.info(f"No --export: would commit files to branch {branch}")
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

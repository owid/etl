"""Export step that commits the OWID Energy dataset to the energy-data repository.

The combined datasets include:
* Statistical review of world energy - Energy Institute.
* International energy data - U.S. Energy Information Administration.
* Energy from fossil fuels - The Shift Dataportal.
* Yearly Electricity Data - Ember.
* Primary energy consumption - Our World in Data.
* Fossil fuel production - Our World in Data.
* Energy mix - Our World in Data.
* Electricity mix - Our World in Data.

Additionally, OWID's regions dataset, population dataset and Maddison Project Database on GDP are included.

Outputs that will be committed to a branch in the energy-data repository:
* The main data file (as a .csv file).
* The codebook (as a .csv file).
* The README file.

"""

import tempfile
from pathlib import Path

import git
import pandas as pd
from structlog import get_logger

from etl.config import DRY_RUN
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
    stat_review_version = [step for step in dependencies if "statistical_review" in step][0].split("/")[-2]
    eia_version = [step for step in dependencies if "eia" in step][0].split("/")[-2]
    shift_version = [step for step in dependencies if "shift" in step][0].split("/")[-2]
    ember_version = [step for step in dependencies if "ember" in step][0].split("/")[-2]
    energy_mix_version = [step for step in dependencies if "energy_mix" in step][0].split("/")[-2]
    electricity_mix_version = [step for step in dependencies if "electricity_mix" in step][0].split("/")[-2]
    primary_energy_version = [step for step in dependencies if "primary_energy" in step][0].split("/")[-2]
    fossil_production = [step for step in dependencies if "fossil_fuel_production" in step][0].split("/")[-2]
    owid_energy_version = [step for step in dependencies if "owid_energy" in step][0].split("/")[-2]

    # Get the versions of the auxiliary steps.
    regions_version = [step for step in dependencies if "regions" in step][0].split("/")[-2]
    population_version = [step for step in dependencies if "population" in step][0].split("/")[-2]
    income_groups_version = [step for step in dependencies if "income_groups" in step][0].split("/")[-2]
    gdp_version = [step for step in dependencies if "maddison" in step][0].split("/")[-2]

    readme = f"""# Data on Energy by *Our World in Data*

Our complete Energy dataset is a collection of key metrics maintained by [*Our World in Data*](https://ourworldindata.org/energy). It is updated regularly and includes data on energy consumption (primary energy, per capita, and growth rates), energy mix, electricity mix and other relevant metrics.

## The complete *Our World in Data* Energy dataset

### 🗂️ Download our complete Energy dataset : [CSV](https://owid-public.owid.io/data/energy/owid-energy-data.csv) | [XLSX](https://owid-public.owid.io/data/energy/owid-energy-data.xlsx) | [JSON](https://owid-public.owid.io/data/energy/owid-energy-data.json)

The CSV and XLSX files follow a format of 1 row per location and year. The JSON version is split by country, with an array of yearly records.

We will continue to publish updated data on energy as it becomes available. Most metrics are published on an annual basis.

A [full codebook](https://github.com/owid/energy-data/blob/master/owid-energy-codebook.csv) is made available, with a description and source for each indicator in the dataset. This codebook is also included as an additional sheet in the XLSX file.

## Our source data and code

The dataset is built upon a number of datasets and processing steps:
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
- Energy from fossil fuels (The Shift Dataportal):
  - [Source data](https://www.theshiftdataportal.org/energy)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/shift/{shift_version}/energy_production_from_fossil_fuels.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/shift/{shift_version}/energy_production_from_fossil_fuels.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/shift/{shift_version}/energy_production_from_fossil_fuels.py)
- Yearly Electricity Data (Ember):
  - [Source data](https://ember-energy.org/data/yearly-electricity-data/)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/ember/{ember_version}/yearly_electricity.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/ember/{ember_version}/yearly_electricity.py)
  - [Further processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/ember/{ember_version}/yearly_electricity.py)
- Energy mix (Our World in Data based on EI's Statistical review of world energy):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy/{energy_mix_version}/energy_mix.py)
- Fossil fuel production (Our World in Data based on EI's Statistical review of world energy & The Shift Dataportal's Energy from fossil fuels):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy/{fossil_production}/fossil_fuel_production.py)
- Primary energy consumption (Our World in Data based on EI's Statistical review of world energy & EIA's International energy data):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy/{primary_energy_version}/primary_energy_consumption.py)
- Electricity mix (Our World in Data based on EI's Statistical Review & Ember's Yearly Electricity Data):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy/{electricity_mix_version}/electricity_mix.py)
- Energy dataset (Our World in Data based on all sources above):
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/energy_data/{owid_energy_version}/owid_energy.py)
  - [Exporting code](https://github.com/owid/etl/blob/master/etl/steps/export/github/energy_data/latest/owid_energy.py)
  - [Uploading code](https://github.com/owid/etl/blob/master/etl/steps/export/s3/energy_data/latest/owid_energy.py)

Additionally, to construct region aggregates and indicators per capita and per GDP, we use the following datasets and processing steps:
- Regions (Our World in Data).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/regions/{regions_version}/regions.py)
- Population (Our World in Data based on [a number of different sources](https://ourworldindata.org/population-sources)).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/demography/{population_version}/population.py)
- Income groups (World Bank).
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/wb/{income_groups_version}/income_groups.py)
- GDP (University of Groningen GGDC's Maddison Project Database, Bolt and van Zanden, 2024).
  - [Source data](https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2023)
  - [Ingestion code](https://github.com/owid/etl/blob/master/snapshots/ggdc/{gdp_version}/maddison_project_database.py)
  - [Basic processing code](https://github.com/owid/etl/blob/master/etl/steps/data/meadow/ggdc/{gdp_version}/maddison_project_database.py)
  - [Processing code](https://github.com/owid/etl/blob/master/etl/steps/data/garden/ggdc/{gdp_version}/maddison_project_database.py)

## Changelog

- On Nov 11, 2025:
  - Moved dataset to different URL.
- On July 17, 2025:
  - Updated the Energy Institute Statistical Review of World Energy.
  - Updated EIA's International energy data.
- On May 30, 2025:
  - Updated Ember's yearly electricity data, which includes data for 2024.
- On September 5, 2024:
  - Added per capita electricity demand, from Ember's yearly electricity data.
- On August 30, 2024:
  - Fixed coal electricity generation for Switzerland, which was missing in the original data, and should be zero instead.
- On June 20, 2024:
  - Updated the Energy Institute Statistical Review of World Energy.
  - Fixed issues on electricity data for aggregate regions.
- On May 8, 2024:
  - Updated Ember's yearly electricity data, which includes data for 2023.
  - Updated GDP data, now coming from Maddison Project Database 2023.
- On January 24, 2024:
  - Improved codebook, to clarify whether indicators refer to electricity generation or primary energy consumption.
  - Improved the calculation of the share of electricity in primary energy. Previously, electricity generation was calculated as a share of input-equivalent primary energy consumption. Now it is calculated as a share of direct primary energy consumption.
- On December 12, 2023:
  - Updated Ember's yearly electricity data and EIA's International energy data.
  - Enhanced codebook (improved descriptions, added units, updated sources).
  - Fixed various minor issues.
- On July 7, 2023:
  - Replaced BP's data by the new Energy Institute Statistical Review of World Energy 2023.
  - Updated Ember's yearly electricity data.
  - Updated all datasets accordingly.
- On June 1, 2023:
  - Updated Ember's yearly electricity data.
  - Renamed countries 'East Timor' and 'Faroe Islands', and added 'Middle East (Ember)'.
  - Population and per capita indicators are now calculated using an updated version of our population dataset.
- On March 1, 2023:
  - Updated Ember's yearly electricity data and fixed some minor issues.
- On December 30, 2022:
  - Fixed some minor issues with BP's dataset. Regions like "Other North America (BP)" have been removed from the data, since, in the original Statistical Review of World Energy, these regions represented different sets of countries for different indicators.
- On December 16, 2022:
  - The column `electricity_share_energy` (electricity as a share of primary energy) was added to the dataset.
  - Fixed some minor inconsistencies in electricity data between Ember and BP, by prioritizing data from Ember.
  - Updated Ember's yearly electricity data.
- On August 9, 2022:
  - All inconsistencies due to different definitions of regions among different datasets (especially Europe) have been fixed.
    - Now all regions follow [Our World in Data's definitions](https://ourworldindata.org/world-region-map-definitions).
    - We also include data for regions as defined in the original datasets; for example, `Europe (BP)` corresponds to Europe as defined by BP.
  - All data processing now occurs outside this repository; the code has been migrated to be part of the [etl repository](https://github.com/owid/etl).
  - Indicator `fossil_cons_per_capita` has been renamed `fossil_elec_per_capita` for consistency, since it corresponds to electricity generation.
  - The codebook has been updated following these changes.
- On April 8, 2022:
  - Electricity data from Ember was updated (using the Global Electricity Review 2022).
  - Data on greenhouse-gas emissions in electricity generation was added (`greenhouse_gas_emissions`).
  - Data on emissions intensity is now provided for most countries in the world.
- On March 25, 2022:
  - Data on net electricity imports and electricity demand was added.
  - BP data was updated (using the Statistical Review of the World Energy 2021).
  - Maddison data on GDP was updated (using the Maddison Project Database 2020).
  - EIA data on primary energy consumption was included in the dataset.
  - Some issues in the dataset were corrected (for example some missing data in production by fossil fuels).
- On February 14, 2022:
  - Some issues were corrected in the electricity data, and the energy dataset was updated accordingly.
  - The json and xlsx dataset files were removed from GitHub in favor of an external storage service, to keep this repository at a reasonable size.
  - The `carbon_intensity_elec` column was added back into the energy dataset.
- On February 3, 2022, we updated the [Ember global electricity data](https://ember-climate.org/data/global-electricity/), combined with the [European Electricity Review from Ember](https://ember-climate.org/project/european-electricity-review-2022/).
  - The `carbon_intensity_elec` column was removed from the energy dataset (since no updated data was available).
  - Columns for electricity from other renewable sources excluding bioenergy were added (namely `other_renewables_elec_per_capita_exc_biofuel`, and `other_renewables_share_elec_exc_biofuel`).
  - Certain countries and regions have been removed from the dataset, because we identified significant inconsistencies in the original data.
- On March 31, 2021, we updated 2020 electricity mix data.
- On September 9, 2020, the first version of this dataset was made available.

## Data processing

- **We standardize names of countries and regions.** Since the names of countries and regions are different in different data sources, we standardize all names in order to minimize data loss during data merges.
- **We create aggregate data for regions (e.g. Africa, Europe, etc.).** Since regions are defined differently by our sources, we create our own aggregates following [*Our World in Data* region definitions](https://ourworldindata.org/world-region-map-definitions).
  - We also include data for regions as defined in the original datasets; for example, `Europe (EI)` corresponds to Europe as defined by the Energy Institute.
- **We recalculate primary energy in terawatt-hours.** The primary data sources on energy—the Energy Institute Statistical review of world energy, for example—typically report consumption in terms of exajoules. We have recalculated these figures as terawatt-hours using a conversion factor of 277.8.
  - Primary energy for renewable sources is reported using [the 'substitution method'](https://ourworldindata.org/energy-substitution-method).
- **We calculate per capita figures.** All of our per capita figures are calculated from our `population` metric, which is included in the complete dataset.
  - We also calculate energy consumption per gdp, and include the corresponding `gdp` metric used in the calculation as part of the dataset.
- **We remove inconsistent data.** Certain data points have been removed because their original data presented anomalies. They may be included again in further data releases if the anomalies are amended.

## License

All visualizations, data, and code produced by _Our World in Data_ are completely open access under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce these in any medium, provided the source and authors are credited.

The data produced by third parties and made available by _Our World in Data_ is subject to the license terms from the original third-party authors. We will always indicate the original source of the data in our database, and you should always check the license of any such third-party data before use.

## Authors

This data has been collected, aggregated, and documented by Pablo Rosado, Hannah Ritchie, Edouard Mathieu, and Max Roser.

The mission of *Our World in Data* is to make data and research on the world's largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).

## How to cite this data?

If you are using this dataset, please cite both [Our World in Data](https://ourworldindata.org/co2-and-greenhouse-gas-emissions#article-citation) and the underlying data source(s).

Please follow [the guidelines in our FAQ](https://ourworldindata.org/faqs#how-should-i-cite-your-data) on how to cite our work.
"""

    # log_dates = re.findall("\d{4}-\d{2}-\d{2}", readme.split("Changelog\n")[-1])
    # error = "Update the change log to add the latest update."
    # assert max(log_dates) >= max([stat_review_version, eia_version, shift_version, ember_version, energy_mix_version, electricity_mix_version, primary_energy_version, fossil_production, owid_energy_version, regions_version,population_version, income_groups_version, gdp_version]), error

    return readme


def run() -> None:
    #
    # Load data.
    #
    # Load the owid_energy dataset from garden, and read its main table.
    ds_energy = paths.load_dataset("owid_energy")
    tb = ds_energy.read("owid_energy")

    #
    # Save outputs.
    #
    # Check if we're on master branch (if so, force dry run)
    branch = git.Repo(BASE_DIR).active_branch.name
    dry_run = DRY_RUN or (branch == "master")

    if branch == "master":
        log.warning("You are on master branch, using dry mode.")
    else:
        log.info(f"Committing files to branch {branch}")

    # Create a temporary directory for all files to be committed.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        # Create codebook and save it as a csv file.
        log.info("Creating codebook csv file.")
        tb.codebook.to_csv(temp_dir_path / "owid-energy-codebook.csv", index=False)

        # Create a csv file.
        log.info("Creating csv file.")
        pd.DataFrame(tb).to_csv(temp_dir_path / "owid-energy-data.csv", index=False, float_format="%.3f")

        # Create a README file.
        log.info("Creating README file.")
        readme = prepare_readme()
        (temp_dir_path / "README.md").write_text(readme)

        repo = GithubApiRepo(repo_name="energy-data")

        repo.create_branch_if_not_exists(branch_name=branch, dry_run=dry_run)

        # Commit csv files to the repo.
        for file_name in ["owid-energy-data.csv", "owid-energy-codebook.csv", "README.md"]:
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
            f"Files committed successfully to branch {branch}. Create a PR here https://github.com/owid/energy-data/compare/master...{branch}."
        )

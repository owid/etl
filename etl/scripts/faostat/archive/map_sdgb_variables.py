"""Map old grapher variables to new ones, and create chart revisions, specifically for faostat_sdgb.

This script has been adapted from create_chart_revisions.py.
We do this separately to the rest of datasets because the item codes of this dataset used to be totally different
to the usual item code. But in the latest version, this issue has been corrected.
Therefore this script will not be necessary after the current update.

"""

import argparse
from typing import Any, Dict, List, Optional

import pandas as pd
from MySQLdb import IntegrityError
from owid.catalog import Dataset
from structlog import get_logger

from etl.chart_revision.v1.revision import create_and_submit_charts_revisions
from etl.paths import DATA_DIR
from etl.scripts.faostat.create_chart_revisions import (
    extract_variables_from_dataset,
    find_and_check_available_versions_for_dataset,
    get_grapher_data_for_old_and_new_variables,
    map_old_to_new_grapher_variable_ids,
)
from etl.scripts.faostat.shared import NAMESPACE

log = get_logger()

# Channel from which the dataset versions and variables will be loaded.
CHANNEL = "grapher"

# Domain and versions of the relevant datasets.
DOMAIN = "sdgb"
VERSION_OLD = "2022-05-17"
VERSION_NEW = "2023-02-22"

# Columns to not take as variables.
COLUMNS_TO_IGNORE = ["country", "year", "index"]

# Some items changed their name, so they have to be manually mapped.
MANUAL_VARIABLES_MAPPING = {
    "12.3.1 Food Loss Percentage (%) | AG_FLS_IDX || Value | 006121 || percent": "12.3.1(a) Food Loss Percentage (%) | 00024044 || Value | 006121 || percent",
    "2.5.2 Proportion of local breeds classified as being at risk as a share of local breeds with known level of extinction risk (%) | ER_RSK_LBREDS || Value | 006121 || percent": "2.5.2 Proportion of local breeds classified as being at risk of extinction (%) | 00024018 || Value | 006121 || percent",
    "6.4.1 Industrial Water Use Efficiency [US$/m3] | 00240281 || Value | 006178 || United States dollars per cubic metre": "6.4.1 Water Use Efficiency [US$/m3] (Industries) | 00240281 || Value | 006178 || United States dollars per cubic metre",
    "6.4.1 Irrigated Agriculture Water Use Efficiency [US$/m3] | 00240280 || Value | 006178 || United States dollars per cubic metre": "6.4.1 Water Use Efficiency [US$/m3] (Agriculture) | 00240280 || Value | 006178 || United States dollars per cubic metre",
    "6.4.1 Services Water Use Efficiency [US$/m3] | 00240282 || Value | 006178 || United States dollars per cubic metre": "6.4.1 Water Use Efficiency [US$/m3] (Services) | 00240282 || Value | 006178 || United States dollars per cubic metre",
    "6.4.1 Total Water Use Efficiency [US$/m3] | 00240283 || Value | 006178 || United States dollars per cubic metre": "6.4.1 Water Use Efficiency [US$/m3] (Total) | 00240283 || Value | 006178 || United States dollars per cubic metre",
    # Level of water stress is disaggregated in Agriculture, Industries, Services, and Total.
    # We assume the old variable (where the aggregate was not specified) corresponds to total.
    "6.4.2 Level of water stress: freshwater withdrawal as a proportion of available freshwater resources (%) | ER_H2O_STRESS || Value | 006121 || percent": "6.4.2 Level of water stress: freshwater withdrawal as a proportion of available freshwater resources (%) (Total) | 00240273 || Value | 006121 || percent",
}


def extract_identifiers_from_variable_name(variable: str) -> Dict[str, Any]:
    # Instead of matching by extracting item and element codes from old and new versions, we do the opposite.
    # We match variables by their name, omitting the codes.
    item, element, unit = variable.split("||")
    item = item.split("|")[0].strip()
    element = element.split("|")[0].strip()
    unit = unit.strip()

    error = f"Item, element or unit could not be extracted for variable: {variable}"
    assert len(item) * len(element) * len(unit) > 0, error
    variable_codes = {"variable": variable, "item": item, "element": element, "unit": unit}

    return variable_codes


def map_old_to_new_variable_names(variables_old: List[str], variables_new: List[str]) -> Dict[str, str]:
    # Extract identifiers from variable names.
    codes_old = pd.DataFrame([extract_identifiers_from_variable_name(variable) for variable in variables_old])
    codes_new = pd.DataFrame([extract_identifiers_from_variable_name(variable) for variable in variables_new])

    variables_matched = pd.merge(
        codes_old, codes_new, how="outer", on=["item", "element", "unit"], suffixes=("_old", "_new")
    )

    # Find if any of the old variables are not found in the new dataset.
    unmatched_old_variables = variables_matched[variables_matched["variable_new"].isnull()]["variable_old"].tolist()

    # Find if there are new variables that did not exist.
    # They could also correspond to old variables that were not successfully matched.
    possible_new_variables = variables_matched[variables_matched["variable_old"].isnull()]["variable_new"].tolist()

    if len(possible_new_variables) > 0:
        log.info(f"There are {len(possible_new_variables)} unknown new variables.")
    if len(unmatched_old_variables) > 0:
        log.info(f"There are {len(unmatched_old_variables)} old variables not matched to any new variables.")

    # Map old variable names to new variable names.
    variables_name_mapping = variables_matched.dropna().set_index("variable_old").to_dict()["variable_new"]

    # Add manually mapped variables.
    variables_name_mapping.update(MANUAL_VARIABLES_MAPPING)

    return variables_name_mapping


def get_grapher_variable_id_mapping_for_two_dataset_versions(
    dataset_short_name: str, version_old: str, version_new: str
) -> Dict[int, int]:

    # Load old and new datasets.
    dataset_old = Dataset(DATA_DIR / "grapher" / NAMESPACE / version_old / dataset_short_name)
    dataset_new = Dataset(DATA_DIR / "grapher" / NAMESPACE / version_new / dataset_short_name)

    # Get all variable names from the old and new datasets.
    variables_old = extract_variables_from_dataset(dataset_short_name=dataset_short_name, version=version_old)
    variables_new = extract_variables_from_dataset(dataset_short_name=dataset_short_name, version=version_new)

    # Map old to new variable names.
    variables_mapping = map_old_to_new_variable_names(variables_old=variables_old, variables_new=variables_new)

    # Get data for old and new variables from grapher db.
    grapher_variables_old, grapher_variables_new = get_grapher_data_for_old_and_new_variables(
        dataset_old=dataset_old, dataset_new=dataset_new
    )

    # Check that variable titles in ETL match those found in grapher DB.
    error = "Mismatch between expected old variable titles in ETL and grapher DB."
    # NOTE: grapher_variables_old includes only variables that have been used in charts, whereas variables_old
    #  includes all variables. Therefore, we check that the former is fully contained in the latter.
    assert set(grapher_variables_old["name"]) <= set(variables_old), error
    error = "Mismatch between expected new variable titles in ETL and grapher DB."
    # NOTE: Both grapher_variables_new and variables_new should contain all variables.
    assert set(grapher_variables_new["name"]) == set(variables_new), error

    grapher_variable_ids_mapping = map_old_to_new_grapher_variable_ids(
        grapher_variables_old, grapher_variables_new, variables_mapping
    )

    return grapher_variable_ids_mapping


def main(
    domains: Optional[List[str]] = None,
    version_old: Optional[str] = None,
    version_new: Optional[str] = None,
    execute_revisions: bool = False,
) -> None:

    if domains is None:
        # If domains is not specified, gather all domains found in all steps for the considered channel.
        domains = sorted(
            set(
                [
                    dataset_path.name.split("_")[-1]
                    for dataset_path in list((DATA_DIR / CHANNEL / NAMESPACE).glob("*/*"))
                ]
            )
        )

    # List all datasets to map.
    dataset_short_names = [f"{NAMESPACE}_{domain.lower()}" for domain in domains]

    for dataset_short_name in dataset_short_names:
        log.info(f"Checking available versions for dataset {dataset_short_name}.")
        # Ensure a dataset exist for each of the specified versions.
        # And if a version is not specified, assume the latest for the new dataset, or second latest for the old.
        version_old, version_new = find_and_check_available_versions_for_dataset(
            dataset_short_name=dataset_short_name, version_old=version_old, version_new=version_new
        )

        # Get mapping of old grapher id variable to new grapher id variable.
        grapher_variable_ids_mapping = get_grapher_variable_id_mapping_for_two_dataset_versions(
            dataset_short_name=dataset_short_name, version_old=version_old, version_new=version_new  # type: ignore
        )

        if execute_revisions and len(grapher_variable_ids_mapping) > 0:
            # Submit revisions to grapher db.
            log.info(f"Creating chart revisions to map {len(grapher_variable_ids_mapping)} old variables to new ones.")
            try:
                create_and_submit_charts_revisions(mapping=grapher_variable_ids_mapping)
            except IntegrityError:
                log.error(
                    "Execution failed because some of the charts are already awaiting revision. "
                    "Go through the approval tool and re-run this script."
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-e",
        "--execute_revisions",
        default=False,
        action="store_true",
        help="If given, execute chart revisions. Otherwise, simply print the log without starting any revisions.",
    )
    args = parser.parse_args()
    main(
        domains=[DOMAIN],
        version_old=VERSION_OLD,
        version_new=VERSION_NEW,
        execute_revisions=args.execute_revisions,
    )

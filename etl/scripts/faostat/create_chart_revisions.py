"""Map old grapher variables to new ones, and create chart revisions, to be able to visually confirm if the variables in
an existing grapher chart can safely be replaced by the new ones.

This script may raise errors, possibly because the chart revision tool fails when trying to create a revision for a
chart that is already in revision (when another variable previously triggered a revision for the same chart).
When this happens, simply go through the approval tool and run this script again until it produces no more revisions.

"""

import argparse
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from MySQLdb import IntegrityError
from owid.catalog import Dataset
from owid.datautils.dataframes import map_series
from structlog import get_logger

from etl import db
from etl.chart_revision.v1.revision import create_and_submit_charts_revisions
from etl.paths import DATA_DIR
from etl.scripts.faostat.shared import NAMESPACE

log = get_logger()

# Channel from which the dataset versions and variables will be loaded.
CHANNEL = "grapher"

# Columns to not take as variables.
COLUMNS_TO_IGNORE = ["country", "year", "index"]

# WARNING: These definitions should coincide with those given in the shared module of the garden step.
# So we will convert it into a string of this number of characters (integers will be prepended with zeros).
N_CHARACTERS_ITEM_CODE = 8
# Maximum number of characters for element_code (integers will be prepended with zeros).
N_CHARACTERS_ELEMENT_CODE = 6

# This regex should extract item codes and element codes, which are made of numbers, sometimes "pc"
# (for per capita variables), and "M" and "F" (for male and female, only for certain domains, like fs and sdgb).
REGEX_TO_EXTRACT_ITEM_AND_ELEMENT = (
    rf".*([0-9pcMF]{{{N_CHARACTERS_ITEM_CODE}}}).*([0-9pcMF]{{{N_CHARACTERS_ELEMENT_CODE}}})"
)


def extract_variables_from_dataset(dataset_short_name: str, version: str) -> List[str]:
    # Load wide table from dataset.
    dataset = Dataset(DATA_DIR / CHANNEL / NAMESPACE / version / dataset_short_name)
    table = dataset[f"{dataset_short_name}_flat"]

    # Get all relevant variable (column) names.
    variable_names = table.reset_index().drop(columns=COLUMNS_TO_IGNORE, errors="ignore").columns.tolist()

    # Get titles for all variables.
    variable_titles = [table[variable].metadata.title for variable in variable_names]

    return variable_titles


def extract_identifiers_from_variable_name(variable: str) -> Dict[str, Any]:
    matches = re.findall(REGEX_TO_EXTRACT_ITEM_AND_ELEMENT, variable)
    error = f"Item code or element code could not be extracted for variable: {variable}"
    assert np.shape(matches) == (1, 2), error
    item_code, element_code = matches[0]
    variable_codes = {"variable": variable, "item_code": item_code, "element_code": element_code}

    return variable_codes


def map_old_to_new_variable_names(variables_old: List[str], variables_new: List[str]) -> Dict[str, str]:
    # Extract item codes and element codes from variable names.
    codes_old = pd.DataFrame([extract_identifiers_from_variable_name(variable) for variable in variables_old])
    codes_new = pd.DataFrame([extract_identifiers_from_variable_name(variable) for variable in variables_new])

    variables_matched = pd.merge(
        codes_old, codes_new, how="outer", on=["item_code", "element_code"], suffixes=("_old", "_new")
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

    return variables_name_mapping


def get_grapher_data_for_old_and_new_variables(
    dataset_old: Dataset, dataset_new: Dataset
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    with db.get_connection() as db_conn:
        # Get old and new dataset ids.
        dataset_id_old = db.get_dataset_id(db_conn=db_conn, dataset_name=dataset_old.metadata.title)
        dataset_id_new = db.get_dataset_id(db_conn=db_conn, dataset_name=dataset_new.metadata.title)

        # Get variables from old dataset that have been used in at least one chart.
        grapher_variables_old = db.get_variables_in_dataset(
            db_conn=db_conn, dataset_id=dataset_id_old, only_used_in_charts=True
        )
        # Get all variables from new dataset.
        grapher_variables_new = db.get_variables_in_dataset(
            db_conn=db_conn, dataset_id=dataset_id_new, only_used_in_charts=False
        )

    return grapher_variables_old, grapher_variables_new


def map_old_to_new_grapher_variable_ids(
    grapher_variables_old: pd.DataFrame, grapher_variables_new: pd.DataFrame, variables_mapping: Dict[str, str]
) -> Dict[int, int]:
    # Add the new variable titles to the old grapher variables dataframe.
    grapher_variables_mapping = grapher_variables_old.copy()
    grapher_variables_mapping["name_new"] = map_series(grapher_variables_mapping["name"], mapping=variables_mapping)

    # Merge with the new grapher variables ensuring that new titles are matched for all variables in the old dataframe.
    grapher_variables_mapping = pd.merge(
        grapher_variables_mapping,
        grapher_variables_new,
        left_on="name_new",
        right_on="name",
        how="left",
        suffixes=("_old", "_new"),
        validate="one_to_one",
    )

    # Map old variable ids to new variable ids.
    grapher_variable_ids_mapping = (
        grapher_variables_mapping[["id_old", "id_new"]].set_index("id_old").to_dict()["id_new"]
    )

    return grapher_variable_ids_mapping


def find_and_check_available_versions_for_dataset(
    dataset_short_name: str, version_old: Optional[str], version_new: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    # Find available versions for current dataset.
    versions_available = [
        dataset_path.parent.name
        for dataset_path in sorted((DATA_DIR / CHANNEL / NAMESPACE).glob(f"*/{dataset_short_name}"))
    ]

    if len(versions_available) < 2:
        log.warning(f"Skipping dataset {dataset_short_name} for which there aren't at least 2 versions.")
        return None, None
    else:
        # If version of new dataset is not specified, take the latest.
        if version_new is None:
            version_new = versions_available[-1]

        # If version of old dataset is not specified, take the second latest.
        if version_old is None:
            version_old = versions_available[-2]

        # If specified version of either old or new dataset do not exist, make version None.
        if not (DATA_DIR / CHANNEL / NAMESPACE / version_old / dataset_short_name).is_dir():
            log.warning(f"Skipping dataset {dataset_short_name} for which version {version_old} is not found.")
            version_old = None
        if not (DATA_DIR / CHANNEL / NAMESPACE / version_new / dataset_short_name).is_dir():
            log.warning(f"Skipping dataset {dataset_short_name} for which version {version_new} is not found.")
            version_new = None

        return version_old, version_new


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
        if (version_old is None) or (version_new is None):
            continue

        # Get mapping of old grapher id variable to new grapher id variable.
        grapher_variable_ids_mapping = get_grapher_variable_id_mapping_for_two_dataset_versions(
            dataset_short_name=dataset_short_name, version_old=version_old, version_new=version_new
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
        "-d",
        "--domains",
        nargs="+",
        help=f"List of domains to include. If not given, all domains found in channel {CHANNEL} will be considered.",
        default=None,
    )
    parser.add_argument(
        "-o",
        "--version_old",
        help=f"Version of the old dataset. If not given, the second latest found in channel {CHANNEL} will be assumed.",
        default=None,
    )
    parser.add_argument(
        "-n",
        "--version_new",
        help=f"Version of the new dataset. If not given, the latest found in channel {CHANNEL} will be assumed.",
        default=None,
    )
    parser.add_argument(
        "-e",
        "--execute_revisions",
        default=False,
        action="store_true",
        help="If given, execute chart revisions. Otherwise, simply print the log without starting any revisions.",
    )
    args = parser.parse_args()
    main(
        domains=args.domains,
        version_old=args.version_old,
        version_new=args.version_new,
        execute_revisions=args.execute_revisions,
    )

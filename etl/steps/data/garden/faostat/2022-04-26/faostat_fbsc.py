"""FAOSTAT: Food Balances Combined.

Combine the old and new food balances datasets:
* Old (historical) dataset: "faostat_fbsh".
* Current dataset: "faostat_fbs".
Into a new (combined) dataset: "faostat_fbsc", and process it like any other FAOSTAT dataset.

This is because a new version of the _Food Balances_ dataset was launched in 2014 with a slightly new methodology:
https://fenixservices.fao.org/faostat/static/documents/FBS/New%20FBS%20methodology.pdf

"""

from copy import deepcopy

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta, TableMeta

from etl.paths import DATA_DIR, STEP_DIR
from etl.scripts.faostat.create_new_steps import find_latest_version_for_step
from .shared import (
    NAMESPACE, VERSION, create_wide_table_with_metadata_from_long_dataframe,
    check_that_flag_definitions_in_dataset_agree_with_those_in_flags_ranking,
    check_that_all_flags_in_dataset_are_in_ranking, clean_data
)

# Dataset name.
DATASET_NAME = f"{NAMESPACE}_fbsc"

# First year for which we have data in fbs dataset (it defines the first year when new methodology is used).
FBS_FIRST_YEAR = 2014
DATASET_TITLE = f"Food Balances (old methodology before {FBS_FIRST_YEAR}, and new from {FBS_FIRST_YEAR} onwards)."

# Path to countries mapping file.
COUNTRIES_FILE = STEP_DIR / "data" / "garden" / NAMESPACE / VERSION / f"{NAMESPACE}.countries.json"

# Some items seem to have been renamed from fbsh to fbs. Ensure the old names are mapped to the new ones.
# TODO: Check that this mapping makes sense.
ITEMS_MAPPING = {
    "Groundnuts (Shelled Eq)": "Groundnuts",
    "Rice (Milled Equivalent)": "Rice and products",
}

# Elements to remove from data.
# TODO: Check that we do not want to keep FAO population.
ELEMENTS_TO_REMOVE = ['Total Population - Both sexes']


def combine_fbsh_and_fbs_datasets(fbsh_dataset, fbs_dataset, additional_metadata):
    # Sanity checks.
    error = "Description of fbs and fbsh datasets is different."
    assert fbsh_dataset.metadata.description == fbs_dataset.metadata.description, error
    error = "Licenses of fbsh and fbs are different."
    assert fbsh_dataset.metadata.licenses == fbs_dataset.metadata.licenses, error

    # Load dataframes for fbs and fbsh datasets.
    fbs = pd.DataFrame(fbs_dataset["faostat_fbs"]).reset_index()
    fbsh = pd.DataFrame(fbsh_dataset["faostat_fbsh"]).reset_index()

    # Ensure there is no overlap in data between the two datasets, and that there is no gap between them.
    assert fbs["year"].min() == FBS_FIRST_YEAR, f"First year of fbs dataset is not {FBS_FIRST_YEAR}"
    if fbsh["year"].max() >= fbs["year"].min():
        print("There is overlapping data between fbsh and fbs datasets. Prioritising fbs over fbsh.")
        fbsh = fbsh[fbsh["year"] < fbs["year"].min()].reset_index(drop=True)
    if (fbsh["year"].max() + 1) < fbs["year"].min():
        print("WARNING: Data is missing for one or more years between fbsh and fbs datasets.")

    # Sanity checks.
    # Ensure the elements that are in fbsh but not in fbs are covered by ITEMS_MAPPING.
    error = "Mismatch between items in fbsh and fbs. Redefine items mapping."
    assert set(fbsh["item"]) - set(fbs["item"]) == set(ITEMS_MAPPING), error
    assert set(fbs["item"]) - set(fbsh["item"]) == set(ITEMS_MAPPING.values()), error
    # Some elements are found in fbs but not in fbsh. This is understandable, since fbs is
    # more recent and may have additional elements. However, ensure that there are no
    # elements in fbsh that are not in fbs.
    error = "There are elements in fbsh that are not in fbs."
    assert set(fbsh["element"]) < set(fbs["element"]), error

    # Add description of each element (from metadata) to fbs and to fbsh.
    # Add also "unit", just to check that data in the original dataset and in metadata coincide.
    fbsh = pd.merge(fbsh, additional_metadata["meta_fbsh_element"].rename(columns={'unit': 'unit_check'}),
                    on="element", how="left")
    fbs = pd.merge(fbs, additional_metadata["meta_fbs_element"].rename(columns={'unit': 'unit_check'}),
                   on="element", how="left")

    # Sanity checks.
    check_that_flag_definitions_in_dataset_agree_with_those_in_flags_ranking(additional_metadata=additional_metadata)
    check_that_all_flags_in_dataset_are_in_ranking(
        data=fbsh, additional_metadata_for_flags=additional_metadata["meta_fbsh_flag"])
    check_that_all_flags_in_dataset_are_in_ranking(
        data=fbs, additional_metadata_for_flags=additional_metadata[f"meta_fbs_flag"])
    # Check that units of elements in fbsh and in the corresponding metadata coincide.
    error = "Elements in fbsh have different units in dataset and in its corresponding metadata."
    assert (fbsh["unit"] == fbsh["unit_check"]).all(), error
    fbsh = fbsh.drop(columns="unit_check")
    # Check that units of elements in fbs and in the corresponding metadata coincide.
    error = "Elements in fbs have different units in dataset and in its corresponding metadata."
    assert (fbs["unit"] == fbs["unit_check"]).all(), error
    fbs = fbs.drop(columns="unit_check")

    # Concatenate old and new dataframes.
    fbsc = pd.concat([fbsh, fbs]).sort_values(["country", "year"]).reset_index(drop=True)

    # Map old item names to new item names.
    fbsc["item"] = fbsc["item"].replace(ITEMS_MAPPING)

    # Remove unnecessary elements.
    fbsc = fbsc[~fbsc["element"].isin(ELEMENTS_TO_REMOVE)].reset_index(drop=True)

    return fbsc


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Find path to latest versions of fbsh dataset.
    fbsh_version = find_latest_version_for_step(channel="meadow", step_name="faostat_fbsh", namespace=NAMESPACE)
    fbsh_file = DATA_DIR / "meadow" / NAMESPACE / fbsh_version / "faostat_fbsh"

    # Find path to latest versions of fbs dataset.
    fbs_version = find_latest_version_for_step(channel="meadow", step_name="faostat_fbs", namespace=NAMESPACE)
    fbs_file = DATA_DIR / "meadow" / NAMESPACE / fbs_version / "faostat_fbs"

    # Find path to latest versions of additional metadata dataset.
    additional_metadata_version = find_latest_version_for_step(
        channel="meadow", step_name="faostat_metadata", namespace=NAMESPACE)
    additional_metadata_file = DATA_DIR / "meadow" / NAMESPACE / additional_metadata_version / "faostat_metadata"

    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Load fbsh, fbs, and additional metadata datasets.
    fbsh_dataset = catalog.Dataset(fbsh_file)
    fbs_dataset = catalog.Dataset(fbs_file)
    additional_metadata = catalog.Dataset(additional_metadata_file)

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    # Combine fbsh and fbs datasets.
    fbsc = combine_fbsh_and_fbs_datasets(fbsh_dataset, fbs_dataset, additional_metadata)

    # Clean data.
    fbsc = clean_data(data=fbsc, countries_file=COUNTRIES_FILE)

    # Create new table for garden dataset.
    table_metadata = TableMeta(short_name=DATASET_NAME, primary_key=["country", "year"])
    fbsc_table = create_wide_table_with_metadata_from_long_dataframe(
        data_long=fbsc, table_metadata=table_metadata)

    ####################################################################################################################
    # Prepare outputs.
    ####################################################################################################################

    # Initialize new garden dataset.
    fbsc_dataset = catalog.Dataset.create_empty(dest_dir)
    # Define metadata for new fbsc garden dataset (by default, take metadata from fbs dataset).
    fbsc_sources = deepcopy(fbs_dataset.metadata.sources[0])
    fbsc_sources.source_data_url = None
    fbsc_sources.owid_data_url = None
    fbsc_dataset.metadata = DatasetMeta(
        namespace=NAMESPACE,
        short_name=DATASET_NAME,
        title=DATASET_TITLE,
        # Take description from any of the datasets (since they should be identical).
        description=fbs_dataset.metadata.description,
        # For sources and licenses, assume those of fbs.
        sources=[fbsc_sources],
        licenses=fbs_dataset.metadata.licenses,
    )
    # Create new dataset in garden.
    fbsc_dataset.save()

    # Add table to dataset.
    fbsc_dataset.add(fbsc_table)

    # TODO: Check why tables for items and elements were added in previous version.

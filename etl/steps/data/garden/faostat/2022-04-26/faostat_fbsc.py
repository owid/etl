"""FAOstat: Food Balances Combined.

Combine the old and new food balances datasets:
* Old (historical) dataset: "faostat_fbsh".
* Current dataset: "faostat_fbs".
Into a new (combined) dataset: "fabosta_fbsc".

This is because a new version of the _Food Balances_ dataset was launched in 2014 with a slightly new methodology
([more info](https://fenixservices.fao.org/faostat/static/documents/FBS/New%20FBS%20methodology.pdf)).

"""

from copy import deepcopy
from typing import List

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta, TableMeta
from owid.datautils import geo

from etl.paths import DATA_DIR, STEP_DIR
from etl.scripts.faostat.create_new_steps import find_latest_version_for_step
from .shared import NAMESPACE, VERSION, FLAGS_RANKING,\
    create_wide_table_with_metadata_from_long_dataframe

# Dataset name.
DATASET_NAME = f"{NAMESPACE}_fbsc"

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


def run(dest_dir: str) -> None:
    # Load latest faostat_fbs dataset from meadow.
    fbs_version = find_latest_version_for_step(channel="meadow", step_name="faostat_fbs", namespace=NAMESPACE)
    fbs_file = DATA_DIR / "meadow" / NAMESPACE / fbs_version / "faostat_fbs"
    fbs_dataset = catalog.Dataset(fbs_file)

    # Load latest faostat_fbsh dataset from meadow.
    fbsh_version = find_latest_version_for_step(channel="meadow", step_name="faostat_fbsh", namespace=NAMESPACE)
    fbsh_file = DATA_DIR / "meadow" / NAMESPACE / fbsh_version / "faostat_fbsh"
    fbsh_dataset = catalog.Dataset(fbsh_file)

    # Sanity checks.
    error = "Description of fbs and fbsh datasets is different."
    assert fbsh_dataset.metadata.description == fbs_dataset.metadata.description, error
    error = "Licenses of fbsh and fbs are different."
    assert fbsh_dataset.metadata.licenses == fbs_dataset.metadata.licenses, error

    # Load latest faostat_metadata from meadow.
    metadata_version = find_latest_version_for_step(channel="meadow", step_name="faostat_metadata", namespace=NAMESPACE)
    metadata_file = DATA_DIR / "meadow" / NAMESPACE / metadata_version / "faostat_metadata"
    metadata_dataset = catalog.Dataset(metadata_file)

    # Load dataframes for fbs and fbsh datasets.
    fbs = pd.DataFrame(fbs_dataset["faostat_fbs"]).reset_index()
    fbsh = pd.DataFrame(fbsh_dataset["faostat_fbsh"]).reset_index()

    # Ensure there is no overlap in data between the two datasets, and that there is no gap 
    # between them.
    if fbsh["year"].max() >= fbs["year"].min():
        print("There is overlapping data between fbsh and fbs datasets. Prioritising fbs over fbsh.")
        fbsh = fbsh[fbsh["year"] < fbs["year"].min()].reset_index(drop=True)
    if (fbsh["year"].max() + 1) < fbs["year"].min():
        print("WARNING: Data is missing for one or more years between fbsh and fbs datasets.")

    # Harmonize country names in both dataframes.
    assert COUNTRIES_FILE.is_file(), "countries file not found."
    fbs = geo.harmonize_countries(df=fbs, countries_file=COUNTRIES_FILE, country_col="area",
                                  warn_on_unused_countries=False).rename(columns={"area": "country"})
    fbsh = geo.harmonize_countries(df=fbsh, countries_file=COUNTRIES_FILE, country_col="area",
                                   warn_on_unused_countries=False).rename(columns={"area": "country"})

    # Remove unused columns.
    unused_columns = ["area_code", "item_code", "element_code"]
    fbs = fbs.drop(columns=unused_columns)
    fbsh = fbsh.drop(columns=unused_columns)

    # There are items in fbsh that are not in fbs and vice versa.
    # We manually created a mapping from old to new names (define above).

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
    fbsh = pd.merge(fbsh, metadata_dataset["meta_fbsh_element"].rename(columns={'unit': 'unit_check'}),
                    on="element", how="left")
    fbs = pd.merge(fbs, metadata_dataset["meta_fbs_element"].rename(columns={'unit': 'unit_check'}),
                   on="element", how="left")

    # Sanity checks.
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

    # Remove unnecessary elements
    fbsc = fbsc[~fbsc["element"].isin(ELEMENTS_TO_REMOVE)].reset_index(drop=True)

    ####################################################################################################################
    # TODO: Remove this temporary solution once grapher accepts mapping of all countries.
    fbsc = fbsc[~fbsc["country"].str.endswith("(FAO)")].reset_index(drop=True)
    ####################################################################################################################

    # Sanity checks.
    # Ensure that each element has only one unit.
    error = "Some elements in the combined dataset have more than one unit."
    assert fbsc.groupby("element")["unit"].nunique().max() == 1, error
    # Ensure that each element has only one unit.
    error = "Some elements in the combined dataset have more than one unit."
    assert fbsc.groupby("element")["description"].nunique().max() == 1, error
    error = "Some elements in the combined dataset have more than one description."
    assert fbsc.groupby("element")["description"].nunique().max() == 1, error

    # Sanity checks.
    def _check_that_flag_definitions_agree_with_those_in_metadata(dataset_name: str) -> None:
        flag_df = metadata_dataset[f"meta_{dataset_name}_flag"].reset_index()
        flag_df["flag"] = flag_df["flag"].astype(str)
        flag_df["flags"] = flag_df["flags"].astype(str)
        comparison = pd.merge(FLAGS_RANKING, flag_df, on="flag", how="inner")
        error = f"Flag definitions in {dataset_name} are different to those in our flag ranking. Redefine flag ranking."
        assert (comparison["description"] == comparison["flags"]).all(), error
    _check_that_flag_definitions_agree_with_those_in_metadata(dataset_name="fbsh")
    _check_that_flag_definitions_agree_with_those_in_metadata(dataset_name="fbs")

    # Check that all flags in the dataset are included in the flags ranking.
    error = "Flags in dataset not found in FLAGS_RANKING. Manually add those flags."
    assert set(fbsc['flag']) < set(FLAGS_RANKING["flag"]), error

    # Add flag ranking to dataset.
    fbsc = pd.merge(fbsc, FLAGS_RANKING[["flag", "ranking"]], on="flag", how="left")

    # Number of ambiguous indices (those that have multiple data values).
    index_columns = ["country", "year", "item", "element", "unit"]
    n_ambiguous_indices = len(fbsc[fbsc.duplicated(subset=index_columns, keep="first")])
    if n_ambiguous_indices > 0:
        # Number of ambiguous indices that cannot be solved using flags.
        n_ambiguous_indices_unsolvable = len(fbsc[
            fbsc.duplicated(subset=index_columns + ["ranking"], keep="first")])
        # Remove ambiguous indices (those that have multiple data values).
        # When possible, use flags to prioritise among duplicates.
        fbsc = fbsc.sort_values(index_columns + ['ranking']).drop_duplicates(
            subset=index_columns, keep="first")
        frac_ambiguous = n_ambiguous_indices / len(fbsc)
        frac_ambiguous_solved_by_flags = 1 - (n_ambiguous_indices_unsolvable / n_ambiguous_indices)
        print(f"Removing {n_ambiguous_indices} ambiguous indices ({frac_ambiguous: .2%}).")
        print(f"{frac_ambiguous_solved_by_flags: .2%} of ambiguities were solved with flags.")

    # Initialize new garden dataset.
    fbsc_dataset = catalog.Dataset.create_empty(dest_dir)

    # Define metadata for new fbsc garden dataset.
    fbsc_sources = deepcopy(fbs_dataset.metadata.sources[0])
    fbsc_sources.source_data_url = None
    fbsc_sources.owid_data_url = None
    fbsc_dataset.metadata = DatasetMeta(
        namespace=NAMESPACE,
        short_name=DATASET_NAME,
        title=f"Food Balances (old methodology from {fbsh['year'].min()} to {fbsh['year'].max()}, and new methodology "
              f"from {fbs['year'].min()} to {fbs['year'].max()}).",
        # Take description from any of the datasets (since they should be identical).
        description=fbs_dataset.metadata.description,
        # For sources and licenses, assume those of fbs.
        sources=fbs_dataset.metadata.sources,
        licenses=fbs_dataset.metadata.licenses,
    )
    # Create new dataset in garden.
    fbsc_dataset.save()

    # Create wide table from long dataframe.
    table_metadata = TableMeta(
        short_name=DATASET_NAME,
        primary_key=["country", "year"],
    )
    fbsc_table = create_wide_table_with_metadata_from_long_dataframe(
        data_long=fbsc, table_metadata=table_metadata)
    # Add table to dataset.
    fbsc_dataset.add(fbsc_table)

    # TODO: Check why tables for items and elements were added in previous version.

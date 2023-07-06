"""FAOSTAT garden step for faostat_fbsc dataset (Food Balances Combined).

Combine the old and new food balances datasets:
* `faostat_fbsh`: Old (historical) dataset.
* `faostat_fbs`: Current dataset.

A new (combined) dataset will be generated: "faostat_fbsc".

This is because a new version of the Food Balances dataset was launched in 2014 with a slightly new methodology:
https://fenixservices.fao.org/faostat/static/documents/FBS/New%20FBS%20methodology.pdf

NOTE: It seems that FAOSTAT is possibly extending the coverage of the new methodology. So the year of intersection of
both datasets will be earlier and earlier. The global variable `FBS_FIRST_YEAR` may have to be redefined in a future
update.

"""

import json
from copy import deepcopy
from typing import cast

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta, TableMeta
from owid.datautils import dataframes
from shared import (
    ADDED_TITLE_TO_WIDE_TABLE,
    LATEST_VERSIONS_FILE,
    NAMESPACE,
    VERSION,
    add_per_capita_variables,
    add_regions,
    clean_data,
    harmonize_elements,
    harmonize_items,
    log,
    prepare_long_table,
    prepare_wide_table,
    remove_outliers,
)

from etl.paths import DATA_DIR, STEP_DIR

# Dataset name.
DATASET_SHORT_NAME = f"{NAMESPACE}_fbsc"

# First year for which we have data in fbs dataset (it defines the first year when new methodology is used).
FBS_FIRST_YEAR = 2010
DATASET_TITLE = f"Food Balances (old methodology before {FBS_FIRST_YEAR}, and new from {FBS_FIRST_YEAR} onwards)"


def combine_fbsh_and_fbs_datasets(
    fbsh_dataset: catalog.Dataset,
    fbs_dataset: catalog.Dataset,
) -> pd.DataFrame:
    """Combine `faostat_fbsh` and `faostat_fbs` meadow datasets.

    Parameters
    ----------
    fbsh_dataset : catalog.Dataset
        Meadow `faostat_fbsh` dataset.
    fbs_dataset : catalog.Dataset
        Meadow `faostat_fbs` dataset.

    Returns
    -------
    fbsc : pd.DataFrame
        Combination of the tables of the two input datasets (as a dataframe, not a dataset).

    """
    # Sanity checks.
    error = "Description of fbs and fbsh datasets is different."
    assert fbsh_dataset.metadata.description == fbs_dataset.metadata.description, error
    error = "Licenses of fbsh and fbs are different."
    assert fbsh_dataset.metadata.licenses == fbs_dataset.metadata.licenses, error

    # Load dataframes for fbs and fbsh datasets.
    fbsh = pd.DataFrame(fbsh_dataset["faostat_fbsh"]).reset_index()
    fbs = pd.DataFrame(fbs_dataset["faostat_fbs"]).reset_index()

    # Harmonize items and elements in both datasets.
    fbsh = harmonize_items(df=fbsh, dataset_short_name="faostat_fbsh")
    fbsh = harmonize_elements(df=fbsh)
    fbs = harmonize_items(df=fbs, dataset_short_name="faostat_fbs")
    fbs = harmonize_elements(df=fbs)

    # Ensure there is no overlap in data between the two datasets, and that there is no gap between them.
    assert fbs["year"].min() == FBS_FIRST_YEAR, f"First year of fbs dataset is not {FBS_FIRST_YEAR}"
    if fbsh["year"].max() >= fbs["year"].min():
        # There is overlapping data between fbsh and fbs datasets. Prioritising fbs over fbsh."
        fbsh = fbsh.loc[fbsh["year"] < fbs["year"].min()].reset_index(drop=True)
    if (fbsh["year"].max() + 1) < fbs["year"].min():
        log.warning("Data is missing for one or more years between fbsh and fbs datasets.")

    # Sanity checks.
    # Ensure the elements that are in fbsh but not in fbs are covered by ITEMS_MAPPING.
    error = "Mismatch between items in fbsh and fbs. Redefine shared.ITEM_AMENDMENTS."
    assert set(fbsh["item"]) == set(fbs["item"]), error
    # Some elements are found in fbs but not in fbsh. This is understandable, since fbs is
    # more recent and may have additional elements. However, ensure that there are no
    # elements in fbsh that are not in fbs.
    error = "There are elements in fbsh that are not in fbs."
    assert set(fbsh["element"]) < set(fbs["element"]), error

    # Concatenate old and new dataframes using function that keeps categoricals.
    fbsc = dataframes.concatenate([fbsh, fbs]).sort_values(["area", "year"]).reset_index(drop=True)

    # Ensure that each element has only one unit and one description.
    error = "Some elements in the combined dataset have more than one unit."
    assert fbsc.groupby("element")["unit"].nunique().max() == 1, error

    return cast(pd.DataFrame, fbsc)


def _assert_df_size(df: pd.DataFrame, size_mb: float) -> None:
    """Check that dataframe is smaller than given size to prevent OOM errors."""
    real_size_mb = df.memory_usage(deep=True).sum() / 1e6
    assert real_size_mb <= size_mb, f"DataFrame size is too big: {real_size_mb} MB > {size_mb} MB"


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Load file of versions.
    latest_versions = pd.read_csv(LATEST_VERSIONS_FILE).set_index(["channel", "dataset"])

    # Find path to latest versions of fbsh dataset.
    fbsh_version = latest_versions.loc["meadow", "faostat_fbsh"].item()
    fbsh_file = DATA_DIR / "meadow" / NAMESPACE / fbsh_version / "faostat_fbsh"
    # Find path to latest versions of fbs dataset.
    fbs_version = latest_versions.loc["meadow", "faostat_fbs"].item()
    fbs_file = DATA_DIR / "meadow" / NAMESPACE / fbs_version / "faostat_fbs"
    # Path to dataset of FAOSTAT metadata.
    garden_metadata_dir = DATA_DIR / "garden" / NAMESPACE / VERSION / f"{NAMESPACE}_metadata"

    # Path to outliers file.
    outliers_file = STEP_DIR / "data" / "garden" / NAMESPACE / VERSION / "detected_outliers.json"

    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Load fbsh and fbs.
    log.info("faostat_fbsc.loading_datasets")
    fbsh_dataset = catalog.Dataset(fbsh_file)
    fbs_dataset = catalog.Dataset(fbs_file)

    # Load dataset of FAOSTAT metadata.
    metadata = catalog.Dataset(garden_metadata_dir)

    # Load and prepare dataset, items and element-units metadata.
    datasets_metadata = pd.DataFrame(metadata["datasets"]).reset_index()
    datasets_metadata = datasets_metadata[datasets_metadata["dataset"] == DATASET_SHORT_NAME].reset_index(drop=True)
    items_metadata = pd.DataFrame(metadata["items"]).reset_index()
    items_metadata = items_metadata[items_metadata["dataset"] == DATASET_SHORT_NAME].reset_index(drop=True)
    elements_metadata = pd.DataFrame(metadata["elements"]).reset_index()
    elements_metadata = elements_metadata[elements_metadata["dataset"] == DATASET_SHORT_NAME].reset_index(drop=True)
    countries_metadata = pd.DataFrame(metadata["countries"]).reset_index()

    # Load file of detected outliers.
    with open(outliers_file, "r") as _json_file:
        outliers = json.loads(_json_file.read())

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    # Combine fbsh and fbs datasets.
    log.info(
        "faostat_fbsc.combine_fbsh_and_fbs_datasets",
        fbsh_shape=fbsh_dataset["faostat_fbsh"].shape,
        fbs_shape=fbs_dataset["faostat_fbs"].shape,
    )
    data = combine_fbsh_and_fbs_datasets(fbsh_dataset, fbs_dataset)

    _assert_df_size(data, 2000)

    # Prepare data.
    data = clean_data(
        data=data,
        items_metadata=items_metadata,
        elements_metadata=elements_metadata,
        countries_metadata=countries_metadata,
    )

    # Add data for aggregate regions.
    data = add_regions(data=data, elements_metadata=elements_metadata)

    # Add per-capita variables.
    data = add_per_capita_variables(data=data, elements_metadata=elements_metadata)

    # Remove outliers from data.
    data = remove_outliers(data, outliers=outliers)

    # Avoid objects as they would explode memory, use categoricals instead.
    for col in data.columns:
        assert data[col].dtype != object, f"Column {col} should not have object type"

    _assert_df_size(data, 2000)

    # Create a long table (with item code and element code as part of the index).
    log.info("faostat_fbsc.prepare_long_table", shape=data.shape)
    data_table_long = prepare_long_table(data=data)

    _assert_df_size(data_table_long, 2000)

    # Create a wide table (with only country and year as index).
    log.info("faostat_fbsc.prepare_wide_table", shape=data.shape)
    data_table_wide = prepare_wide_table(data=data)

    ####################################################################################################################
    # Prepare outputs.
    ####################################################################################################################

    log.info("faostat_fbsc.prepare_outputs")

    # Initialize new garden dataset.
    dataset_garden = catalog.Dataset.create_empty(dest_dir)
    # Define metadata for new fbsc garden dataset (by default, take metadata from fbs dataset).
    fbsc_sources = deepcopy(fbs_dataset.metadata.sources[0])
    fbsc_sources.source_data_url = None
    fbsc_sources.owid_data_url = None
    # Check that the title assigned here coincides with the one in custom_datasets.csv (for consistency).
    error = "Dataset title given to fbsc is different to the one in custom_datasets.csv. Update the latter file."
    assert DATASET_TITLE == datasets_metadata["owid_dataset_title"].item(), error
    dataset_garden_metadata = DatasetMeta(
        namespace=NAMESPACE,
        short_name=DATASET_SHORT_NAME,
        title=DATASET_TITLE,
        # Take description from any of the datasets (since they should be identical).
        description=datasets_metadata["owid_dataset_description"].item(),
        # For sources and licenses, assume those of fbs.
        sources=[fbsc_sources],
        licenses=fbs_dataset.metadata.licenses,
        version=VERSION,
    )
    dataset_garden.metadata = dataset_garden_metadata
    # Create new dataset in garden.
    dataset_garden.save()

    # Prepare metadata for new garden long table.
    data_table_long.metadata = TableMeta(short_name=DATASET_SHORT_NAME)
    data_table_long.metadata.title = dataset_garden_metadata.title
    data_table_long.metadata.description = dataset_garden_metadata.description
    # Add long table to the dataset (no need to repack, since columns already have optimal dtypes).
    dataset_garden.add(data_table_long, repack=False)

    # Prepare metadata for new garden wide table (starting with the metadata from the long table).
    data_table_wide.metadata = deepcopy(data_table_long.metadata)
    data_table_wide.metadata.title += ADDED_TITLE_TO_WIDE_TABLE
    data_table_wide.metadata.short_name += "_flat"
    data_table_wide.metadata.primary_key = list(data_table_wide.index.names)
    # Add wide table to the dataset (no need to repack, since columns already have optimal dtypes).
    dataset_garden.add(data_table_wide, repack=False)

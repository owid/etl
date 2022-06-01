from copy import deepcopy
from pathlib import Path
from typing import Iterable

import pandas as pd
from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR, STEP_DIR
from etl.scripts.faostat.create_new_steps import find_latest_version_for_step

# TODO: I need to harmonize item codes again here because etl stores string columns of integer characters as an
#  integer column.
# Maximum number of characters for item_code.
N_CHARACTERS_ITEM_CODE = 8
# Maximum number of characters for element_code (integers will be prepended with zeros).
N_CHARACTERS_ELEMENT_CODE = 6


def remove_columns_that_only_have_nans(
    data: pd.DataFrame, verbose: bool = True
) -> pd.DataFrame:
    """Remove columns that only have nans.

    In principle, it should not be possible that columns have only nan values, but we use this function just in case.

    Parameters
    ----------
    data : pd.DataFrame
        Data for current dataset.
    verbose : bool
        True to print information about the removal of columns with nan values.

    Returns
    -------
    data : pd.DataFrame
        Data after removing columns of nans.

    """
    data = data.copy()
    # Remove columns that only have nans.
    columns_of_nans = data.columns[data.isnull().all(axis=0)]
    if len(columns_of_nans) > 0:
        if verbose:
            print(
                f"Removing {len(columns_of_nans)} columns ({len(columns_of_nans) / len(data.columns): .2%}) "
                f"that have only nans."
            )
        data = data.drop(columns=columns_of_nans)

    return data


def customize_names_in_data(data, items_metadata, elements_metadata):
    data["item_code"] = data["item_code"].astype(str).str.zfill(N_CHARACTERS_ITEM_CODE)
    data["element_code"] = data["element_code"].astype(str).str.zfill(N_CHARACTERS_ELEMENT_CODE)

    error = f"There are missing item codes in metadata."
    assert set(data["item_code"]) <= set(items_metadata["item_code"]), error

    error = f"There are missing element codes in metadata."
    assert set(data["element_code"]) <= set(elements_metadata["element_code"]), error

    _expected_n_rows = len(data)

    data = pd.merge(data.rename(columns={"item": "fao_item"}),
                    items_metadata[['item_code', 'owid_item', 'owid_item_description']], on="item_code", how="left")
    assert len(data) == _expected_n_rows, f"Something went wrong when merging data with items metadata."

    data = pd.merge(data.rename(columns={"element": "fao_element", "unit": "fao_unit"}),
                    elements_metadata[['element_code', 'owid_element', 'owid_unit', 'owid_unit_factor',
                                       'owid_element_description', 'owid_unit_description']],
                    on=["element_code"], how="left")
    assert len(data) == _expected_n_rows, f"Something went wrong when merging data with elements metadata."

    # Ensure columns have the right type.
    # For some reason, doing .astype(str) kills the kernel.
    # data["owid_item_description"] = data["owid_item_description"].astype(str)
    # data["owid_element_description"] = data["owid_element_description"].astype(str)
    # data["owid_unit_description"] = data["owid_unit_description"].astype(str)
    data["owid_item_description"] = [i for i in data["owid_item_description"]]
    data["owid_element_description"] = [i for i in data["owid_element_description"]]
    data["owid_unit_description"] = [i for i in data["owid_unit_description"]]

    # Select necessary columns, and sort conveniently.
    data = data[['country', 'year', 'owid_item', 'owid_element', 'owid_unit', 'value',
                 'owid_item_description', 'owid_element_description', 'owid_unit_description', 'owid_unit_factor']]
    data = data.sort_values(["country", "year", "owid_item", "owid_element"]).reset_index(drop=True)

    # Remove "owid_" from column names.
    data = data.rename(columns={column: column.replace("owid_", "") for column in data.columns})

    return data


def prepare_data_table(dataset: catalog.Dataset, metadata: catalog.Dataset) -> catalog.Table:
    """Prepare multi-index garden table to have a grapher-friendly format.

    The input table will be pivoted to have [country, year] as index, and as many columns as combinations of
    item-element-unit entities.

    Parameters
    ----------
    dataset : catalog.Dataset
        Current dataset, containing one table.

    Returns
    -------
    wide_table : catalog.Table
        Data table with index [country, year].

    """
    # By construction there should only be one table in each dataset. Load that table.
    assert len(dataset.table_names) == 1, "Expected only one table inside the dataset."
    # The name of the only table should coincide with the original dataset short name (e.g. faostat_XXX).
    table_name = dataset.table_names[0]
    long_table = dataset[table_name]
    data = pd.DataFrame(long_table).reset_index()

    # Load and prepare items and element-units metadata.
    items_metadata = pd.DataFrame(metadata["items"]).reset_index()
    items_metadata = items_metadata[items_metadata["dataset"] == table_name].reset_index(drop=True)
    items_metadata["item_code"] = items_metadata["item_code"].astype(str).str.zfill(N_CHARACTERS_ITEM_CODE)
    elements_metadata = pd.DataFrame(metadata["elements"]).reset_index()
    elements_metadata = elements_metadata[elements_metadata["dataset"] == table_name].reset_index(drop=True)
    elements_metadata["element_code"] = elements_metadata["element_code"].astype(str).str.zfill(N_CHARACTERS_ELEMENT_CODE)

    # Add custom names to data, and add description columns.
    data_long = customize_names_in_data(data, items_metadata, elements_metadata)

    table_metadata = deepcopy(long_table.metadata)

    # Combine item, element and unit into one column.
    if "item" not in data_long.columns:
        data_long["item"] = ""

    data_long["title"] = (
        data_long["item"].astype(str)
        + " - "
        + data_long["element"].astype(str)
        + " ("
        + data_long["unit"].astype(str)
        + ")"
    )

    # Create a column with item and element descriptions.
    data_long["description"] = ""
    rows_with_element_description_mask = data_long["element_description"] != ""
    data_long.loc[rows_with_element_description_mask, "description"] += data_long[rows_with_element_description_mask]["element_description"] + "\n"
    rows_with_item_description_mask = data_long["item_description"] != ""
    data_long.loc[rows_with_item_description_mask, "description"] += data_long[rows_with_item_description_mask]["item_description"]

    # TODO: Is there a quicker way to achieve the same result, maybe just with one pivot?

    # Keep a dataframe of just units (which will be required later on).
    unit_short_names = data_long.pivot(index=["country", "year"], columns=["title"], values="unit")

    unit_long_names = data_long.pivot(index=["country", "year"], columns=["title"], values="unit_description")
    unit_factors = data_long.pivot(index=["country", "year"], columns=["title"], values="unit_factor")

    # Keep a dataframe of just variable descriptions (which will be required later on).
    descriptions = data_long.pivot(index=["country", "year"], columns=["title"], values="description")

    # This will create a table with just one column and country-year as index.
    data = data_long.pivot(index=["country", "year"], columns=["title"], values="value")

    # Remove columns that only have nans.
    data = remove_columns_that_only_have_nans(data)

    # Sort data columns and rows conveniently.
    data = data[sorted(data.columns)]
    data = data.sort_index(level=["country", "year"])

    # Create new table for garden dataset.
    wide_table = catalog.Table(data).copy()
    # Each column will become a new variable of the grapher dataset.
    # Create the required metadata for each variable.
    for column in wide_table.columns:
        # Unit (long name) for this variable.
        unit_long_name = unit_long_names[column].dropna().unique()
        assert len(unit_long_name) == 1, f"Variable {column} has ambiguous unit descriptions."
        unit_long_name = unit_long_name[0]
        wide_table[column].metadata.unit = unit_long_name

        # Unit (short name) for this variable.
        unit_short_name = unit_short_names[column].dropna().unique()
        assert len(unit_short_name) == 1, f"Variable {column} has ambiguous units."
        unit_short_name = unit_short_name[0]
        wide_table[column].metadata.short_unit = unit_short_name

        # Conversion factor to apply to the unit (it can be empty, in which case it will be ignored).
        unit_factor = unit_factors[column].dropna().unique()
        if len(unit_factor) > 0:
            assert len(unit_factor) == 1, f"Variable {column} has ambiguous unit conversion factors."
            unit_factor = unit_factor[0]
            wide_table[column].metadata.display = {"conversionFactor": unit_factor}

        # Description for this variable.
        description = descriptions[column].dropna().unique()
        assert len(description) == 1, f"Variable {column} has ambiguous descriptions."
        description = description[0]
        wide_table[column].metadata.description = description

        # Remove unit from title (only last occurrence of the unit).
        title = " ".join(column.rsplit(f" ({unit_long_name})", 1)).strip()
        wide_table[column].metadata.title = title

    # Make all column names snake_case.
    wide_table = catalog.utils.underscore_table(wide_table)

    # Use the same table metadata as from original meadow table, but update index.
    wide_table.metadata = deepcopy(table_metadata)
    wide_table.metadata.primary_key = ["country", "year"]

    # Reset index, since countries will need to be converted into entities.
    wide_table = wide_table.reset_index()

    return wide_table


def get_grapher_dataset_from_file_name(file_path: str) -> catalog.Dataset:
    """Get dataset that needs to be inserted into grapher, given a path to a grapher step.

    Parameters
    ----------
    file_path : Path or str
        Path to code of grapher step being executed.

    Returns
    -------
    dataset : catalog.Dataset
        Latest version of the garden dataset to be inserted into grapher.

    """
    # Get details of this grapher step from the file path.
    namespace, grapher_version, file_name = Path(file_path).parts[-3:]
    dataset_short_name = file_name.split(".")[0]
    # Get details of the corresponding latest garden step.
    garden_version = find_latest_version_for_step(
        channel="garden", step_name=dataset_short_name, namespace=namespace
    )
    dataset = catalog.Dataset(
        DATA_DIR / "garden" / namespace / garden_version / dataset_short_name
    )

    # Get metadata for current dataset from garden (to get a description for the dataset).
    metadata = get_metadata(dataset)

    # Short name for new grapher dataset.
    dataset.metadata.short_name = f"{dataset_short_name}__{grapher_version}".replace(
        "-", "_"
    )
    # Add dataset description (taken from metadata).
    dataset.metadata.description = metadata["datasets"].loc[dataset_short_name]["owid_dataset_description"]

    # move description to source as that is what is shown in grapher
    # (dataset.description would be displayed under `Internal notes` in the admin UI otherwise)
    dataset.metadata.sources[0].description = dataset.metadata.description
    dataset.metadata.description = ""

    return dataset


def get_metadata(dataset: catalog.Dataset) -> catalog.Dataset:
    """Get faostat_metadata dataset from garden for current dataset version.

    Parameters
    ----------
    dataset : catalog.Dataset
        Current dataset.

    Returns
    -------
    metadata : catalog.Dataset
        Latest version of the garden faostat_metadata dataset.
    """
    # Get metadata for current dataset from garden.
    dataset_version = dataset.metadata.version
    #################################################################
    # TODO: Remove this once meadow datasets are created with the correct metadata.version
    #  (once convert_walden_metadata is updated to receive "version").
    dataset_version = find_latest_version_for_step(
        channel="garden", step_name=dataset.metadata.short_name.split("__")[0], namespace="faostat"
    )
    #################################################################
    metadata_dir = DATA_DIR / "garden" / "faostat" / dataset_version / "faostat_metadata"
    assert metadata_dir.is_dir(), f"Metadata dataset not found."
    metadata = catalog.Dataset(metadata_dir)

    return metadata


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    """Yield each of the columns of the table of a dataset, with a format that is ready to be inserted into grapher.

    This function will also create all entities in grapher that do not already exist.

    Parameters
    ----------
    dataset : catalog.Dataset
        Dataset containing only one table, that will be split into many tables (one per column).

    Yields
    ------
    table : catalog.Table
        Each iteration yields a new table with index [entity_id, year] and only one column. This is done for each column
        in the original table of the dataset.

    """
    # Get metadata for current dataset from garden.
    metadata = get_metadata(dataset)

    # Create a wide table.
    table = prepare_data_table(dataset=dataset, metadata=metadata)

    # Convert country names into grapher entity ids, and set index appropriately.
    # WARNING: This will create new entities in grapher if not already existing.
    table["entity_id"] = gh.country_to_entity_id(table["country"], create_entities=True)
    table = table.set_index(["entity_id", "year"]).drop(columns=["country"])

    yield from gh.yield_wide_table(table, na_action="drop")

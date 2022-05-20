from copy import deepcopy
from pathlib import Path
from typing import Iterable

import pandas as pd
from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR
from etl.scripts.faostat.create_new_steps import find_latest_version_for_step


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


def prepare_wide_table(data_table: catalog.Table) -> catalog.Table:
    """Prepare multi-index garden table to have a grapher-friendly format.

    The input table will be pivoted to have [country, year] as index, and as many columns as combinations of
    item-element-unit entities.

    Parameters
    ----------
    data_table : catalog.Table
        Data table for current dataset, usually with index [country, year, item, element, unit].

    Returns
    -------
    wide_table : catalog.Table
        Data table with index [country, year].

    """
    data_long = pd.DataFrame(data_table).reset_index()
    table_metadata = deepcopy(data_table.metadata)

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

    # Keep a dataframe of just units (which will be required later on).
    units = data_long.pivot(index=["country", "year"], columns=["title"], values="unit")

    # This will create a table with just one column and country-year as index.
    data = data_long.pivot(index=["country", "year"], columns=["title"], values="value")

    # Remove columns that only have nans.
    data = remove_columns_that_only_have_nans(data)

    # Sort data columns and rows conveniently.
    data = data[sorted(data.columns)]
    data = data.sort_index(level=["country", "year"])

    # Create new table for garden dataset.
    wide_table = catalog.Table(data).copy()
    for column in wide_table.columns:
        variable_units = units[column].dropna().unique()
        assert len(variable_units) == 1, f"Variable {column} has ambiguous units."
        unit = variable_units[0]
        # Remove unit from title (only last occurrence of the unit).
        title = " ".join(column.rsplit(f" ({unit})", 1)).strip()

        # Add title and unit to each column in the table.
        wide_table[column].metadata.title = title
        wide_table[column].metadata.unit = unit

    # Make all column names snake_case.
    wide_table = catalog.utils.underscore_table(wide_table)

    # Use the same table metadata as from original meadow table, but update index.
    wide_table.metadata = deepcopy(table_metadata)
    wide_table.metadata.primary_key = ["country", "year"]

    # TODO: Check why in food_explorer, _fields are also added to the table.
    # data_table_garden._fields = fields

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
    # Short name for new grapher dataset.
    dataset.metadata.short_name = f"{dataset_short_name}__{grapher_version}".replace(
        "-", "_"
    )

    # move description to source as that is what is shown in grapher
    # (dataset.description would be displayed under `Internal notes` in the admin UI otherwise)
    dataset.metadata.sources[0].description = dataset.metadata.description
    dataset.metadata.description = ""

    return dataset


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
    # By construction there should only be one table in each dataset. Load that table.
    assert len(dataset.table_names) == 1, "Expected only one table inside the dataset."
    table_name = dataset.table_names[0]
    table_long = dataset[table_name]

    # Create a wide table.
    table = prepare_wide_table(data_table=table_long)

    # Convert country names into grapher entity ids, and set index appropriately.
    # WARNING: This will create new entities in grapher if not already existing.
    table["entity_id"] = gh.country_to_entity_id(table["country"], create_entities=True)
    table = table.set_index(["entity_id", "year"]).drop(columns=["country"])

    yield from gh.yield_wide_table(table, na_action="drop")

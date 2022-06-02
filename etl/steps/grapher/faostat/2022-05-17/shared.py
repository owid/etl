"""Common grapher step for all FAOSTAT domains.

"""
from pathlib import Path
from typing import Iterable

from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR


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

    # Find latest garden dataset for current FAOSTAT domain.
    garden_version = sorted((DATA_DIR / "garden" / namespace).glob(f"*/{dataset_short_name}"))[-1].parent.name

    # Load latest garden dataset.
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
    # Fetch wide table from dataset.
    flat_table_names = [table_name for table_name in dataset.table_names if table_name.endswith("_flat")]
    assert len(flat_table_names) == 1
    table = dataset[flat_table_names[0]].reset_index().drop(columns=["area_code"])

    # Convert country names into grapher entity ids, and set index appropriately.
    # WARNING: This will create new entities in grapher if not already existing.
    table["entity_id"] = gh.country_to_entity_id(table["country"], create_entities=True)
    table = table.set_index(["entity_id", "year"]).drop(columns=["country"])

    yield from gh.yield_wide_table(table, na_action="drop")

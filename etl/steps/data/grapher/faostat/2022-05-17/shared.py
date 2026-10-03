"""Common grapher step for all FAOSTAT domains."""

from pathlib import Path

import pandas as pd
from owid import catalog

from etl.paths import DATA_DIR, STEP_DIR


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

    # Path to file containing information of the latest versions of the relevant datasets.
    latest_versions_file = STEP_DIR / "data/grapher" / namespace / grapher_version / "versions.csv"

    # Load file of versions.
    latest_versions = pd.read_csv(latest_versions_file).set_index(["channel", "dataset"])

    # Path to latest dataset in garden for current FAOSTAT domain.
    garden_version = latest_versions.loc["garden", dataset_short_name].item()
    garden_data_dir = DATA_DIR / "garden" / namespace / garden_version / dataset_short_name

    # Load latest garden dataset.
    dataset = catalog.Dataset(garden_data_dir)

    # Some datasets have " - FAO (YYYY)" at the end, and some others do not.
    # For consistency, remove that ending of the title, and add something consistent across all datasets.
    dataset.metadata.title = dataset.metadata.title.split(" - FAO (")[0] + f" (FAO, {grapher_version})"

    return dataset


def get_grapher_table(dataset: catalog.Dataset) -> catalog.Table:
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

    return table

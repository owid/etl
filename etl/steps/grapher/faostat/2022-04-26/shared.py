from pathlib import Path
from typing import Iterable

from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR
from etl.scripts.faostat.create_new_steps import find_latest_version_for_step


def get_grapher_dataset_from_file_name(file_path: str) -> catalog.Dataset:
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
    # By construction there should only be one table in each dataset. Load that table.
    assert len(dataset.table_names) == 1
    table_name = dataset.table_names[0]
    table = dataset[table_name].reset_index()

    # Convert country names into grapher entity ids, and set index appropriately.
    table["entity_id"] = gh.country_to_entity_id(table["country"], create_entities=True)
    table = table.set_index(["entity_id", "year"]).drop(columns=["country"])

    yield from gh.yield_wide_table(table, na_action="drop")

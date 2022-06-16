"""Imports a dataset and associated data sources, variables, and data points
into the SQL database.

Usage:

    >>> from standard_importer import import_dataset
    >>> dataset_dir = "worldbank_wdi"
    >>> dataset_namespace = "worldbank_wdi@2021.05.25"
    >>> import_dataset.main(dataset_dir, dataset_namespace)
"""

from dataclasses import dataclass
import json
import os
import pandas as pd
from typing import Dict, List, cast, Optional
from contextlib import contextmanager
from collections.abc import Generator

from etl.db import get_connection
from etl.db_utils import DBUtils
from etl import config
from owid import catalog
from owid.catalog import utils

import logging
import traceback

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


CURRENT_DIR = os.path.dirname(__file__)
# CURRENT_DIR = os.path.join(os.getcwd(), 'standard_importer')

INT_TYPES = (
    "int",
    "uint64",
    "Int64",
)


@dataclass
class DatasetUpsertResult:
    dataset_id: int
    source_ids: Dict[str, int]


@dataclass
class VariableUpsertResult:
    variable_id: int
    source_id: int


def upsert_dataset(
    dataset: catalog.Dataset,
    namespace: str,
    sources: List[catalog.meta.Source],
    source_checksum: str,
) -> DatasetUpsertResult:
    utils.validate_underscore(dataset.metadata.short_name, "Dataset's short_name")

    if len(sources) > 1:
        raise NotImplementedError(
            "only a single source is supported for grapher datasets, use `join_sources` to join multiple sources"
        )

    # This function creates the dataset table row, a namespace row
    # and the sources table row(s). There is a bit of an open question if we should
    # map one dataset with N tables to one namespace and N datasets in
    # mysql or if we should just flatten it into one dataset?
    with open_db() as db:
        print("Verifying namespace is present")
        ns = db.fetch_one_or_none("SELECT * from namespaces where name=%s", [namespace])
        if ns is None:
            db.upsert_namespace(namespace, "")

        print("Upserting dataset")
        dataset_id = db.upsert_dataset(
            dataset.metadata.short_name,
            namespace,
            int(cast(str, config.GRAPHER_USER_ID)),
            source_checksum=source_checksum,
            description=dataset.metadata.description or "",
        )

        source_ids: Dict[str, int] = dict()
        for source in sources:
            source_ids[source.name] = _upsert_source_to_db(db, source, dataset_id)

        return DatasetUpsertResult(dataset_id, source_ids)


def _upsert_source_to_db(db: DBUtils, source: catalog.Source, dataset_id: int) -> int:
    """Upsert source and return its id"""
    if source.name is None:
        raise ValueError("Source name was None - please fix this in the metadata.")

    json_description = json.dumps(
        {
            "link": source.url,
            "retrievedDate": source.date_accessed,
            "dataPublishedBy": source.published_by,
            "dataPublisherSource": source.publisher_source,
            # NOTE: we remap `description` to additionalInfo since that is what is shown as `Description` in
            # the admin UI. Clean this up with the new data model
            "additionalInfo": source.description,
        }
    )
    return db.upsert_source(source.name, json_description, dataset_id)


def _update_variables_display(table: catalog.Table) -> None:
    """Grapher uses units from field `display` instead of fields `unit` and `short_unit`
    before we fix grapher data model, copy them to `display`.
    """
    for col in table.columns:
        meta = table[col].metadata
        meta.display = meta.display or {}
        meta.display.setdefault("shortUnit", meta.short_unit)
        if meta.unit:
            meta.display.setdefault("unit", meta.unit)


def upsert_table(
    table: catalog.Table, dataset_upsert_result: DatasetUpsertResult
) -> VariableUpsertResult:
    """This function is used to put one ready to go formatted Table (i.e.
    in the format (year, entityId, value)) into mysql. The metadata
    of the variable is used to fill the required fields.
    """

    assert set(table.index.names) == {
        "year",
        "entity_id",
    }, f"Tables to be upserted must have only 2 indices: year and entity_id. Instead they have: {table.index.names}"
    assert (
        len(table.columns) == 1
    ), f"Tables to be upserted must have only 1 column. Instead they have: {table.columns.names}"
    assert table[
        table.columns[0]
    ].title, f"Column {table.columns.names} must have a title in metadata"
    assert (
        table.iloc[:, 0].notnull().all()
    ), f"Tables to be upserted must have no null values. Instead they have:\n{table.loc[table.iloc[:, 0].isnull()]}"
    table = table.reorder_levels(["year", "entity_id"])
    assert (
        table.index.dtypes[0] in INT_TYPES
    ), f"year must be of an integer type but was: {table.index.dtypes[0]}"
    assert (
        table.index.dtypes[1] in INT_TYPES
    ), f"entity_id must be of an integer type but was: {table.index.dtypes[1]}"
    utils.validate_underscore(table.metadata.short_name, "Table's short_name")
    utils.validate_underscore(table.columns[0], "Variable's name")

    if len(table.iloc[:, 0].metadata.sources) > 1:
        raise NotImplementedError(
            "only a single source is supported for grapher variables, use `join_sources` to join multiple sources"
        )

    _update_variables_display(table)

    with open_db() as db:
        logger.info("---Upserting variable...")

        # For easy retrieveal of the value series we store the name
        column_name = table.columns[0]

        years = table.index.unique(level="year").values
        min_year = min(years)
        max_year = max(years)
        timespan = f"{min_year}-{max_year}"

        table.reset_index(inplace=True)

        # Every variable must have a source. Use variable source if specified, otherwise use dataset source
        if len(table[column_name].metadata.sources) > 0:
            source = table[column_name].metadata.sources[0]

            # Does it already exist in the database?
            source_id = dataset_upsert_result.source_ids.get(source.name)
            if not source_id:
                # Not exists, upsert it
                # NOTE: this could be quite inefficient as we upsert source for every variable
                #   optimize this if this turns out to be a bottleneck
                source_id = _upsert_source_to_db(
                    db, source, dataset_upsert_result.dataset_id
                )
        else:
            # Use dataset source
            source_id = list(dataset_upsert_result.source_ids.values())[0]

        db_variable_id = db.upsert_variable(
            name=table[column_name].title,
            source_id=source_id,
            dataset_id=dataset_upsert_result.dataset_id,
            description=table[column_name].metadata.description,
            code=None,
            unit=table[column_name].metadata.unit,
            short_unit=table[column_name].metadata.short_unit,
            timespan=timespan,
            coverage="",
            display=table[column_name].metadata.display,
            original_metadata=None,
        )

        db.cursor.execute(
            """
            DELETE FROM data_values WHERE variableId=%s
        """,
            [int(db_variable_id)],
        )

        query = """
            INSERT INTO data_values
                (value, year, entityId, variableId)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                year = VALUES(year),
                entityId = VALUES(entityId),
                variableId = VALUES(variableId)
        """
        db.upsert_many(
            query,
            (
                (row[column_name], row["year"], row["entity_id"], db_variable_id)
                for index, row in table.iterrows()
            ),
        )
        logger.info(f"Upserted {len(table)} datapoints.")

        return VariableUpsertResult(db_variable_id, source_id)


def fetch_db_checksum(dataset: catalog.Dataset) -> Optional[str]:
    """
    Fetch the latest source checksum associated with a given dataset in the db. Can be compared
    with the current source checksum to determine whether the db is up-to-date.
    """
    with open_db() as db:
        source_checksum = db.fetch_one_or_none(
            """
            SELECT sourceChecksum
            FROM datasets
            WHERE name=%s
            """,
            [dataset.metadata.short_name],
        )
        return None if source_checksum is None else source_checksum[0]


def cleanup_ghost_variables(dataset_id: int, upserted_variable_ids: List[int]) -> None:
    """Remove all leftover variables that didn't get upserted into DB during grapher step.
    This could happen when you rename or delete a variable in ETL.
    Raise an error if we try to delete variable used by any chart.

    :param dataset_id: ID of the dataset
    :param upserted_variable_ids: variables upserted in grapher step
    """
    with open_db() as db:
        # get all those variables first
        db.cursor.execute(
            """
            SELECT id FROM variables WHERE datasetId=%(dataset_id)s AND id NOT IN %(variable_ids)s
        """,
            {"dataset_id": dataset_id, "variable_ids": upserted_variable_ids},
        )
        rows = db.cursor.fetchall()

        variable_ids_to_delete = [row[0] for row in rows]

        # nothing to delete, quit
        if not variable_ids_to_delete:
            return

        # raise an exception if they're used in any charts
        db.cursor.execute(
            """
            SELECT chartId, variableId FROM chart_dimensions WHERE variableId IN %(variable_ids)s
        """,
            {"dataset_id": dataset_id, "variable_ids": variable_ids_to_delete},
        )
        rows = db.cursor.fetchall()
        if rows:
            rows = pd.DataFrame(rows, columns=["chartId", "variableId"])
            raise ValueError(
                f"Variables used in charts will not be deleted automatically:\n{rows}"
            )

        # first delete data_values
        db.cursor.execute(
            """
            DELETE FROM data_values WHERE variableId IN %(variable_ids)s
        """,
            {"variable_ids": variable_ids_to_delete},
        )

        # then variables themselves
        db.cursor.execute(
            """
            DELETE FROM variables WHERE datasetId=%(dataset_id)s AND id IN %(variable_ids)s
        """,
            {"dataset_id": dataset_id, "variable_ids": variable_ids_to_delete},
        )

        logging.warning(
            f"Deleted {db.cursor.rowcount} ghost variables ({variable_ids_to_delete})"
        )


def cleanup_ghost_sources(dataset_id: int, upserted_source_ids: List[int]) -> None:
    """Remove all leftover sources that didn't get upserted into DB during grapher step.
    This could happen when you rename or delete sources.
    :param dataset_id: ID of the dataset
    :param upserted_source_ids: sources upserted in grapher step
    """
    with open_db() as db:
        db.cursor.execute(
            """
            DELETE FROM sources WHERE datasetId=%(dataset_id)s AND id NOT IN %(source_ids)s
        """,
            {"dataset_id": dataset_id, "source_ids": upserted_source_ids},
        )
        if db.cursor.rowcount > 0:
            logging.warning(f"Deleted {db.cursor.rowcount} ghost sources")


@contextmanager
def open_db() -> Generator[DBUtils, None, None]:
    connection = None
    cursor = None
    try:
        connection = get_connection()
        connection.autocommit(False)
        cursor = connection.cursor()
        yield DBUtils(cursor)
        connection.commit()
    except Exception as e:
        logger.error(f"Error encountered during import: {e}")
        logger.error("Rolling back changes...")
        if connection:
            connection.rollback()
        if config.DEBUG:
            traceback.print_exc()
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

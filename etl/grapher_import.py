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
from typing import Dict, List, cast

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


def upsert_dataset(
    dataset: catalog.Dataset, namespace: str, sources: List[catalog.meta.Source]
) -> DatasetUpsertResult:
    utils.validate_underscore(dataset.metadata.short_name, "Dataset's short_name")

    # This function creates the dataset table row, a namespace row
    # and the sources table row(s). There is a bit of an open question if we should
    # map one dataset with N tables to one namespace and N datasets in
    # mysql or if we should just flatten it into one dataset?
    connection = None
    cursor = None
    try:
        connection = get_connection()
        connection.autocommit(False)
        cursor = connection.cursor()
        db = DBUtils(cursor)

        print("Verifying namespace is present")
        ns = db.fetch_one_or_none("SELECT * from namespaces where name=%s", [namespace])
        if ns is None:
            db.upsert_namespace(namespace, "")

        print("Upserting dataset")
        dataset_id = db.upsert_dataset(
            dataset.metadata.short_name,
            namespace,
            int(cast(str, config.GRAPHER_USER_ID)),
            description=dataset.metadata.description,
        )

        source_ids: Dict[str, int] = dict()
        for source in sources:
            if source.name is not None:
                json_description = json.dumps(
                    {
                        "link": source.url,
                        "retrievedDate": source.date_accessed,
                        "dataPublishedBy": source.name,
                    }
                )
                source_id = db.upsert_source(source.name, json_description, dataset_id)
                source_ids[source.name] = source_id
            else:
                print(
                    "Source name was None - please fix this in the metadata. Continuing"
                )
        connection.commit()

        return DatasetUpsertResult(dataset_id, source_ids)
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


def upsert_table(
    table: catalog.Table, dataset_upsert_result: DatasetUpsertResult
) -> None:
    # This function is used to put one ready to go formatted Table (i.e.
    # in the format (year, entityId, value)) into mysql. The metadata
    # of the variable is used to fill the required fields

    assert set(table.index.names) == {
        "year",
        "entity_id",
    }, f"Tables to be upserted must have only 2 indices: year and entity_id. Instead they have: {table.index.names}"
    assert (
        len(table.columns) == 1
    ), f"Tables to be upserted must have only 1 column. Instead they have: {table.columns.names}"
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
    utils.validate_underscore(
        table.iloc[:, 0].metadata.short_name, "Variable's short_name"
    )

    connection = None
    cursor = None
    try:

        connection = get_connection()
        connection.autocommit(False)
        cursor = connection.cursor()
        db = DBUtils(cursor)

        logger.info("---Upserting variable...")

        # For easy retrieveal of the value series we store the name
        column_name = table.columns[0]

        years = table.index.unique(level="year").values
        min_year = min(years)
        max_year = max(years)
        timespan = f"{min_year}-{max_year}"

        table.reset_index(inplace=True)

        source_id = None
        if len(table[column_name].metadata.sources) > 0:
            source_name = table[column_name].metadata.sources[0].name
            if source_name is not None:
                source_id = dataset_upsert_result.source_ids.get(source_name)
        if source_id is None:
            source_id = list(dataset_upsert_result.source_ids.values())[0]

        db_variable_id = db.upsert_variable(
            name=table.metadata.short_name,
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

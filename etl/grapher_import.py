"""Imports a dataset and associated data sources, variables, and data points
into the SQL database.

Usage:

    >>> from standard_importer import import_dataset
    >>> dataset_dir = "worldbank_wdi"
    >>> dataset_namespace = "worldbank_wdi@2021.05.25"
    >>> import_dataset.main(dataset_dir, dataset_namespace)
"""

import concurrent.futures
import json
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, cast

import pandas as pd
import structlog
from owid import catalog
from owid.catalog import utils
from sqlalchemy import Integer, String
from tenacity import retry, stop

from etl import config
from etl.db import get_engine, open_db
from etl.db_utils import DBUtils

from . import grapher_helpers as gh

log = structlog.get_logger()


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


def upsert_dataset(dataset: catalog.Dataset, namespace: str, sources: List[catalog.meta.Source]) -> DatasetUpsertResult:
    assert dataset.metadata.short_name, "Dataset must have a short_name"
    assert dataset.metadata.version, "Dataset must have a version"
    assert dataset.metadata.title, "Dataset must have a title"

    utils.validate_underscore(dataset.metadata.short_name, "Dataset's short_name")

    if len(sources) > 1:
        raise NotImplementedError(
            "only a single source is supported for grapher datasets, use"
            " `combine_metadata_sources` or `adapt_dataset_metadata_for_grapher` to"
            " join multiple sources"
        )

    # This function creates the dataset table row, a namespace row
    # and the sources table row(s). There is a bit of an open question if we should
    # map one dataset with N tables to one namespace and N datasets in
    # mysql or if we should just flatten it into one dataset?
    with open_db() as db:
        log.info("upsert_dataset.verify_namespace", namespace=namespace)
        ns = db.fetch_one_or_none("SELECT * from namespaces where name=%s", [namespace])
        if ns is None:
            db.upsert_namespace(namespace, "")
        else:
            _, ns_name, _, ns_is_archived = ns
            if ns_is_archived:
                log.warning("upsert_dataset.namespace_is_archived", namespace=ns_name)

        log.info(
            "upsert_dataset.upsert_dataset.start",
            short_name=dataset.metadata.short_name,
        )
        dataset_id = db.upsert_dataset(
            dataset.metadata.short_name,
            dataset.metadata.title,
            dataset.metadata.version,
            namespace,
            int(cast(str, config.GRAPHER_USER_ID)),
            description=dataset.metadata.description or "",
        )
        log.info(
            "upsert_dataset.upsert_dataset.end",
            short_name=dataset.metadata.short_name,
            id=dataset_id,
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


def upsert_table(table: catalog.Table, dataset_upsert_result: DatasetUpsertResult) -> VariableUpsertResult:
    """This function is used to put one ready to go formatted Table (i.e.
    in the format (year, entityId, value)) into mysql. The metadata
    of the variable is used to fill the required fields.
    """

    assert set(table.index.names) == {"year", "entity_id"}, (
        "Tables to be upserted must have only 2 indices: year and entity_id. Instead" f" they have: {table.index.names}"
    )
    assert len(table.columns) == 1, (
        "Tables to be upserted must have only 1 column. Instead they have:" f" {table.columns.names}"
    )
    assert table[table.columns[0]].title, f"Column `{table.columns[0]}` must have a title in metadata"
    assert table.iloc[:, 0].notnull().all(), (
        "Tables to be upserted must have no null values. Instead they" f" have:\n{table.loc[table.iloc[:, 0].isnull()]}"
    )
    table = table.reorder_levels(["year", "entity_id"])
    assert table.index.dtypes[0] in INT_TYPES, f"year must be of an integer type but was: {table.index.dtypes[0]}"
    assert table.index.dtypes[1] in INT_TYPES, f"entity_id must be of an integer type but was: {table.index.dtypes[1]}"
    utils.validate_underscore(table.metadata.short_name, "Table's short_name")
    utils.validate_underscore(table.columns[0], "Variable's name")

    if len(table.iloc[:, 0].metadata.sources) > 1:
        raise NotImplementedError(
            "only a single source is supported for grapher datasets, use"
            " `combine_metadata_sources` or `adapt_dataset_metadata_for_grapher` to"
            " join multiple sources"
        )

    assert not gh.contains_inf(table.iloc[:, 0]), f"Column `{table.columns[0]}` has inf values"

    _update_variables_display(table)

    with open_db() as db:
        log.info("upsert_table.upsert_variable", variable=table.columns[0])

        # For easy retrieveal of the value series we store the name
        column_name = table.columns[0]

        years = table.index.unique(level="year").values
        min_year = min(years)
        max_year = max(years)
        timespan = f"{min_year}-{max_year}"

        table.reset_index(inplace=True)

        # Every variable must have exactly one source
        if len(table[column_name].metadata.sources) != 1:
            raise NotImplementedError(
                f"Variable `{column_name}` must have exactly one source, see function"
                " `adapt_table_for_grapher` that can do that for you"
            )

        source = table[column_name].metadata.sources[0]

        # Does it already exist in the database?
        source_id = dataset_upsert_result.source_ids.get(source.name)
        if not source_id:
            # Not exists, upsert it
            # NOTE: this could be quite inefficient as we upsert source for every variable
            #   optimize this if this turns out to be a bottleneck
            source_id = _upsert_source_to_db(db, source, dataset_upsert_result.dataset_id)

        db_variable_id = db.upsert_variable(
            short_name=column_name,
            title=table[column_name].title,
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

    df = table.rename(columns={column_name: "value", "entity_id": "entityId"}).assign(variableId=db_variable_id)

    insert_to_data_values(df)

    log.info("upsert_table.upserted_data_values", size=len(table))

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
            WHERE shortName=%s
            """,
            [dataset.metadata.short_name],
        )
        return None if source_checksum is None else source_checksum[0]


def set_dataset_checksum(dataset_id: int, checksum: str) -> None:
    with open_db() as db:
        db.cursor.execute(
            """
            UPDATE datasets
            SET sourceChecksum = %s
            WHERE id=%s
        """,
            [checksum, dataset_id],
        )


def cleanup_ghost_variables(dataset_id: int, upserted_variable_ids: List[int], workers: int = 1) -> None:
    """Remove all leftover variables that didn't get upserted into DB during grapher step.
    This could happen when you rename or delete a variable in ETL.
    Raise an error if we try to delete variable used by any chart.

    :param dataset_id: ID of the dataset
    :param upserted_variable_ids: variables upserted in grapher step
    :param workers: delete variables in parallel
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

        log.info("cleanup_ghost_variables.start", size=len(variable_ids_to_delete))

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
            raise ValueError(f"Variables used in charts will not be deleted automatically:\n{rows}")

        # first delete data_values
        # NOTE: deleting 100 variables takes ~30s with 10 workers with threading
        # and about ~3mins when deleting them in batch
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(_delete_variable_from_data_values, variable_ids_to_delete))

        # then variables themselves
        db.cursor.execute(
            """
            DELETE FROM variables WHERE datasetId=%(dataset_id)s AND id IN %(variable_ids)s
        """,
            {"dataset_id": dataset_id, "variable_ids": variable_ids_to_delete},
        )

        log.warning(
            "cleanup_ghost_variables.end",
            size=db.cursor.rowcount,
            variables=variable_ids_to_delete,
        )


def _delete_variable_from_data_values(variable_id: int) -> None:
    with open_db() as db:
        db.cursor.execute(
            """
                    DELETE FROM data_values WHERE variableId = %(variable_id)s
                """,
            {"variable_id": variable_id},
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
            log.warning(f"Deleted {db.cursor.rowcount} ghost sources")


@retry(stop=stop.stop_after_attempt(3))
def insert_to_data_values(df: pd.DataFrame) -> None:
    """Insert data into data_values table. Retry in case we get Deadlock error."""
    # insert data to data_values using pandas which is both faster and doesn't raise
    # deadlocks
    df.to_sql(
        "data_values",
        get_engine(),
        if_exists="append",
        index=False,
        dtype={
            "value": String(255),
            "year": Integer(),
            "entityId": Integer(),
            "variableId": Integer(),
        },
    )

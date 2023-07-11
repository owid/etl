"""Imports a dataset and associated data sources, variables, and data points
into the SQL database.

Usage:

    >>> from standard_importer import import_dataset
    >>> dataset_dir = "worldbank_wdi"
    >>> dataset_namespace = "worldbank_wdi@2021.05.25"
    >>> import_dataset.main(dataset_dir, dataset_namespace)
"""

import datetime
import os
from dataclasses import dataclass
from threading import Lock
from typing import Dict, List, Optional, cast

import pandas as pd
import structlog
from owid import catalog
from owid.catalog import utils
from sqlalchemy.engine.base import Engine
from sqlmodel import Session, select, update

from backport.datasync.data_metadata import (
    add_entity_code_and_name,
    variable_data,
    variable_metadata,
)
from backport.datasync.datasync import upload_gzip_dict
from etl import config
from etl.db import open_db

from . import grapher_helpers as gh
from . import grapher_model as gm

log = structlog.get_logger()


source_table_lock = Lock()


CURRENT_DIR = os.path.dirname(__file__)


@dataclass
class DatasetUpsertResult:
    dataset_id: int
    source_ids: Dict[str, int]


@dataclass
class VariableUpsertResult:
    variable_id: int
    source_id: int


def upsert_dataset(
    engine: Engine, dataset: catalog.Dataset, namespace: str, sources: List[catalog.meta.Source]
) -> DatasetUpsertResult:
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

    short_name = dataset.metadata.short_name

    # This function creates the dataset table row, a namespace row
    # and the sources table row(s). There is a bit of an open question if we should
    # map one dataset with N tables to one namespace and N datasets in
    # mysql or if we should just flatten it into one dataset?
    with Session(engine) as session:
        log.info("upsert_dataset.verify_namespace", namespace=namespace)
        ns = gm.Namespace(name=namespace, description="")
        ns = ns.upsert(session)
        if ns.isArchived:
            log.warning("upsert_dataset.namespace_is_archived", namespace=ns.name)

        log.info(
            "upsert_dataset.upsert_dataset.start",
            short_name=short_name,
        )
        ds = gm.Dataset.from_dataset_metadata(
            dataset.metadata, namespace=namespace, user_id=int(cast(str, config.GRAPHER_USER_ID))
        ).upsert(session)

        session.commit()

        assert ds.id
        if ds.isArchived:
            log.warning(
                "upsert_dataset.dataset_is_archived",
                id=ds.id,
                short_name=short_name,
            )

        log.info(
            "upsert_dataset.upsert_dataset.end",
            short_name=short_name,
            id=ds.id,
        )

        source_ids: Dict[str, int] = dict()
        for source in sources:
            assert source.name
            source_ids[source.name] = _upsert_source_to_db(session, source, ds.id)

        session.commit()

        return DatasetUpsertResult(ds.id, source_ids)


def _upsert_source_to_db(session: Session, source: catalog.Source, dataset_id: int) -> int:
    """Upsert source and return its id"""
    # NOTE: we need the lock because upserts can happen in multiple threads and `sources` table
    # has no unique constraint on `name`. It can be removed once we switch to variable views
    # and stop using threads
    with source_table_lock:
        db_source = gm.Source.from_catalog_source(source, dataset_id).upsert(session)

        # commit within the lock to make sure other threads get the latest sources
        session.commit()

        assert db_source.id
        return db_source.id


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
    engine: Engine,
    table: catalog.Table,
    dataset_upsert_result: DatasetUpsertResult,
    catalog_path: Optional[str] = None,
    dimensions: Optional[gm.Dimensions] = None,
) -> VariableUpsertResult:
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
    assert table.index.dtypes[0] in gh.INT_TYPES, f"year must be of an integer type but was: {table.index.dtypes[0]}"
    assert (
        table.index.dtypes[1] in gh.INT_TYPES
    ), f"entity_id must be of an integer type but was: {table.index.dtypes[1]}"
    utils.validate_underscore(table.metadata.short_name, "Table's short_name")
    utils.validate_underscore(table.columns[0], "Variable's name")

    # make sure we have unique (year, entity_id) pairs
    vc = table.index.value_counts()
    if (vc > 1).any():
        raise AssertionError(f"Duplicate (year, entity_id) pairs:\n {vc[vc > 1].index.tolist()}")

    if len(table.iloc[:, 0].metadata.sources) > 1:
        raise NotImplementedError(
            "only a single source is supported for grapher datasets, use"
            " `combine_metadata_sources` or `adapt_dataset_metadata_for_grapher` to"
            " join multiple sources"
        )

    assert not gh.contains_inf(table.iloc[:, 0]), f"Column `{table.columns[0]}` has inf values"

    _update_variables_display(table)

    with Session(engine) as session:
        # For easy retrieveal of the value series we store the name
        column_name = table.columns[0]

        years = table.index.unique(level="year").values
        if len(years) == 0:
            timespan = ""
        else:
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
            source_id = _upsert_source_to_db(session, source, dataset_upsert_result.dataset_id)

        variable = gm.Variable.from_variable_metadata(
            table[column_name].metadata,
            short_name=column_name,
            timespan=timespan,
            dataset_id=dataset_upsert_result.dataset_id,
            source_id=source_id,
            catalog_path=catalog_path,
            dimensions=dimensions,
        ).upsert(session)
        variable_id = variable.id
        assert variable_id

        df = table.rename(columns={column_name: "value", "entity_id": "entityId"})

        # following functions assume that `value` is string
        df["value"] = df["value"].astype(str)

        # NOTE: we could prefetch all entities in advance, but it's not a bottleneck as it takes
        # less than 10ms per variable
        df = add_entity_code_and_name(engine, df)

        # process and upload data to S3
        var_data = variable_data(df)
        data_path = upload_gzip_dict(var_data, variable.s3_data_path(), r2=True)

        # we need to commit changes because we use SQL command in `variable_metadata`. We wouldn't
        # have to if we used ORM instead
        session.add(variable)
        session.commit()

        # process and upload metadata to S3
        var_metadata = variable_metadata(engine, variable_id, df)
        metadata_path = upload_gzip_dict(var_metadata, variable.s3_metadata_path(), r2=True)

        variable.dataPath = data_path
        variable.metadataPath = metadata_path
        session.add(variable)
        session.commit()

        log.info("upsert_table.upserted_variable", size=len(table), id=variable_id, title=variable.name)

        return VariableUpsertResult(variable_id, source_id)  # type: ignore


def fetch_db_checksum(dataset: catalog.Dataset) -> Optional[str]:
    """
    Fetch the latest source checksum associated with a given dataset in the db. Can be compared
    with the current source checksum to determine whether the db is up-to-date.
    """
    assert dataset.metadata.short_name, "Dataset must have a short_name"
    assert dataset.metadata.version, "Dataset must have a version"
    assert dataset.metadata.namespace, "Dataset must have a namespace"

    with Session(gm.get_engine()) as session:
        q = select(gm.Dataset).where(
            gm.Dataset.shortName == dataset.metadata.short_name,
            gm.Dataset.version == dataset.metadata.version,
            gm.Dataset.namespace == dataset.metadata.namespace,
        )
        ds = session.exec(q).one_or_none()
        return ds.sourceChecksum if ds is not None else None


def set_dataset_checksum_and_editedAt(dataset_id: int, checksum: str) -> None:
    with Session(gm.get_engine()) as session:
        q = (
            update(gm.Dataset)
            .where(gm.Dataset.id == dataset_id)
            .values(
                sourceChecksum=checksum,
                dataEditedAt=datetime.datetime.utcnow(),
                metadataEditedAt=datetime.datetime.utcnow(),
            )
        )
        session.execute(q)
        session.commit()


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

        # there might still be some data_values for old variables
        db.cursor.execute(
            """
            DELETE FROM data_values WHERE variableId IN %(variable_ids)s
        """,
            {"variable_ids": variable_ids_to_delete},
        )

        # then variables themselves with related data in other tables
        db.cursor.execute(
            """
            DELETE FROM country_latest_data WHERE variable_id IN %(variable_ids)s
        """,
            {"variable_ids": variable_ids_to_delete},
        )
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

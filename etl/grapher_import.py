"""Imports a dataset and associated data sources, variables, and data points
into the SQL database.

Usage:

    >>> from standard_importer import import_dataset
    >>> dataset_dir = "worldbank_wdi"
    >>> dataset_namespace = "worldbank_wdi@2021.05.25"
    >>> import_dataset.main(dataset_dir, dataset_namespace)
"""

import datetime
import json
import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock
from typing import Dict, List, Optional, cast

import pandas as pd
import structlog
from owid import catalog
from owid.catalog import utils
from sqlalchemy import select, text, update
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

from apps.backport.datasync import data_metadata as dm
from apps.backport.datasync.datasync import upload_gzip_string
from etl import config
from etl.db import get_engine

from . import grapher_helpers as gh
from . import grapher_model as gm

log = structlog.get_logger()


source_table_lock = Lock()
origins_table_lock = Lock()


CURRENT_DIR = os.path.dirname(__file__)


@dataclass
class DatasetUpsertResult:
    dataset_id: int
    source_ids: Dict[int, int]


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

        source_ids: Dict[int, int] = dict()
        for source in sources:
            assert source.name
            source_ids[hash(source)] = _upsert_source_to_db(session, source, ds.id)

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


def _add_or_update_source(
    session: Session, variable_meta: catalog.VariableMeta, column_name: str, dataset_upsert_result: DatasetUpsertResult
) -> Optional[int]:
    if not variable_meta.sources:
        assert variable_meta.origins, "Variable must have either sources or origins"
        return None

    # Every variable must have exactly one source
    if len(variable_meta.sources) != 1:
        raise NotImplementedError(
            f"Variable `{column_name}` must have exactly one source, see function"
            " `adapt_table_for_grapher` that can do that for you"
        )

    source = variable_meta.sources[0]

    # Does it already exist in the database?
    assert source.name
    source_id = dataset_upsert_result.source_ids.get(hash(source))
    if not source_id:
        # Not exists, upsert it
        # NOTE: this could be quite inefficient as we upsert source for every variable
        #   luckily we are moving away from sources towards origins
        source_id = _upsert_source_to_db(session, source, dataset_upsert_result.dataset_id)

    return source_id


def _add_or_update_origins(session: Session, origins: list[catalog.Origin]) -> list[gm.Origin]:
    out = []
    assert len(origins) == len(set(origins)), "origins must be unique"
    for origin in origins:
        out.append(gm.Origin.from_origin(origin).upsert(session))
    return out


def _update_variables_metadata(table: catalog.Table) -> None:
    """Update variables metadata."""
    for col in table.columns:
        meta = table[col].metadata

        # Grapher uses units from field `display` instead of fields `unit` and `short_unit`
        # before we fix grapher data model, copy them to `display`.
        meta.display = meta.display or {}
        if meta.short_unit:
            meta.display.setdefault("shortUnit", meta.short_unit)
        if meta.unit:
            meta.display.setdefault("unit", meta.unit)

        # Prune empty fields from description_key
        if meta.description_key:
            meta.description_key = [k for k in meta.description_key if k.strip()]


def upsert_table(
    engine: Engine,
    table: catalog.Table,
    dataset_upsert_result: DatasetUpsertResult,
    catalog_path: Optional[str] = None,
    dimensions: Optional[gm.Dimensions] = None,
    verbose: bool = True,
) -> VariableUpsertResult:
    """This function is used to put one ready to go formatted Table (i.e.
    in the format (year, entityId, value)) into mysql. The metadata
    of the variable is used to fill the required fields.
    """

    # We sometimes get a warning, but it's unclear where it is coming from
    # Passing a BlockManager to Table is deprecated and will raise in a future version. Use public APIs instead.
    warnings.filterwarnings("ignore", category=DeprecationWarning)

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
    assert (
        table.index.dtypes.iloc[0] in gh.INT_TYPES
    ), f"year must be of an integer type but was: {table.index.dtypes.iloc[0]}"
    assert (
        table.index.dtypes.iloc[1] in gh.INT_TYPES
    ), f"entity_id must be of an integer type but was: {table.index.dtypes.iloc[1]}"
    utils.validate_underscore(table.metadata.short_name, "Table's short_name")
    utils.validate_underscore(table.columns[0], "Variable's name")

    # make sure we have unique (year, entity_id) pairs
    vc = table.index.value_counts()
    if (vc > 1).any():
        with Session(engine) as session:
            duplicates = [
                (year, entity_id, _get_entity_name(session, entity_id)) for year, entity_id in vc[vc > 1].index.tolist()
            ]
        raise AssertionError(f"Duplicates (year, entity_id, entity_name):\n {duplicates}")

    if len(table.iloc[:, 0].metadata.sources) > 1:
        raise NotImplementedError(
            "only a single source is supported for grapher datasets, use"
            " `combine_metadata_sources` or `adapt_dataset_metadata_for_grapher` to"
            " join multiple sources"
        )

    assert not gh.contains_inf(table.iloc[:, 0]), f"Column `{table.columns[0]}` has inf values"

    _update_variables_metadata(table)

    with Session(engine) as session:
        # For easy retrieveal of the value series we store the name
        column_name = table.columns[0]
        variable_meta: catalog.VariableMeta = table[column_name].metadata

        # Timespan does not work for yearIsDay variables
        if (variable_meta.display or {}).get("yearIsDay"):
            timespan = ""
        else:
            years = table.index.unique(level="year").values
            if len(years) == 0:
                timespan = ""
            else:
                min_year = min(years)
                max_year = max(years)
                timespan = f"{min_year}-{max_year}"

        # sort table to get deterministic checksum later on
        table = table.sort_index()

        table.reset_index(inplace=True)

        source_id = _add_or_update_source(session, variable_meta, column_name, dataset_upsert_result)

        with origins_table_lock:
            db_origins = _add_or_update_origins(session, variable_meta.origins)
            # commit within the lock to make sure other threads get the latest sources
            session.commit()

        db_variable = gm.Variable.from_variable_metadata(
            variable_meta,
            short_name=column_name,
            timespan=timespan,
            dataset_id=dataset_upsert_result.dataset_id,
            source_id=source_id,
            catalog_path=catalog_path,
            dimensions=dimensions,
        ).upsert(session)
        db_variable_id = db_variable.id
        assert db_variable_id

        df = table.rename(columns={column_name: "value", "entity_id": "entityId"})

        # following functions assume that `value` is string
        # NOTE: we could make the code more efficient if we didn't convert `value` to string
        df["value"] = df["value"].astype(str)

        if not db_variable.type:
            db_variable.type = db_variable.infer_type(df["value"])

        # NOTE: we could prefetch all entities in advance, but it's not a bottleneck as it takes
        # less than 10ms per variable
        df = dm.add_entity_code_and_name(session, df)

        # update links, we need to do it after we commit deleted relationships above
        db_variable.update_links(
            session,
            db_origins,
            faqs=variable_meta.presentation.faqs if variable_meta.presentation else [],
            tag_names=variable_meta.presentation.topic_tags if variable_meta.presentation else [],
        )
        session.add(db_variable)

        # we need to commit changes because we use SQL command in `variable_metadata`. We wouldn't
        # have to if we used ORM instead
        session.commit()

        # process data and metadata
        var_data = dm.variable_data(df)
        var_metadata = dm.variable_metadata(session, db_variable_id, df)

        var_data_str = json.dumps(var_data, default=str)
        var_metadata_str = json.dumps(var_metadata, default=str)

        checksum_data = dm.checksum_data_str(var_data_str)
        # NOTE: _checksum_metadata modifies `var_metadata` object, but we have it as a string already
        checksum_metadata = dm.checksum_metadata(var_metadata)

        # upload them to R2
        with ThreadPoolExecutor() as executor:
            futures = []

            if db_variable.dataChecksum != checksum_data:
                db_variable.dataChecksum = checksum_data
                futures.append(executor.submit(upload_gzip_string, var_data_str, db_variable.s3_data_path()))

            if db_variable.metadataChecksum != checksum_metadata:
                db_variable.metadataChecksum = checksum_metadata
                futures.append(executor.submit(upload_gzip_string, var_metadata_str, db_variable.s3_metadata_path()))

            # commit new checksums
            if futures:
                # Wait for futures to complete in case exceptions are raised
                [f.result() for f in futures]

                session.add(db_variable)
                session.commit()

        if verbose:
            if futures:
                log.info("upsert_table.uploaded_to_s3", size=len(table), variable_id=db_variable_id)
            else:
                log.info("upsert_table.skipped_upload_to_s3", size=len(table), variable_id=db_variable_id)

        return VariableUpsertResult(db_variable_id, source_id)  # type: ignore


def fetch_db_checksum(dataset: catalog.Dataset) -> Optional[str]:
    """
    Fetch the latest source checksum associated with a given dataset in the db. Can be compared
    with the current source checksum to determine whether the db is up-to-date.
    """
    assert dataset.metadata.short_name, "Dataset must have a short_name"
    assert dataset.metadata.version, "Dataset must have a version"
    assert dataset.metadata.namespace, "Dataset must have a namespace"

    with Session(get_engine()) as session:
        q = select(gm.Dataset).where(
            gm.Dataset.shortName == dataset.metadata.short_name,
            gm.Dataset.version == dataset.metadata.version,
            gm.Dataset.namespace == dataset.metadata.namespace,
        )
        ds = session.scalars(q).one_or_none()
        return ds.sourceChecksum if ds is not None else None


def set_dataset_checksum_and_editedAt(dataset_id: int, checksum: str) -> None:
    with Session(get_engine()) as session:
        q = (
            update(gm.Dataset)
            .where(gm.Dataset.id == dataset_id)  # type: ignore
            .values(
                sourceChecksum=checksum,
                dataEditedAt=datetime.datetime.utcnow(),
                metadataEditedAt=datetime.datetime.utcnow(),
            )
        )
        session.execute(q)
        session.commit()


def cleanup_ghost_variables(engine: Engine, dataset_id: int, upserted_variable_ids: List[int]) -> bool:
    """Remove all leftover variables that didn't get upserted into DB during grapher step.
    This could happen when you rename or delete a variable in ETL.
    Raise an error if we try to delete variable used by any chart.

    :param dataset_id: ID of the dataset
    :param upserted_variable_ids: variables upserted in grapher step
    :param workers: delete variables in parallel

    :return: True if successful
    """
    with engine.connect() as con:
        # get all those variables first
        rows = con.execute(
            text(
                """
            SELECT id FROM variables WHERE datasetId = :dataset_id AND id NOT IN :variable_ids
        """
            ),
            {"dataset_id": dataset_id, "variable_ids": upserted_variable_ids or [-1]},
        ).fetchall()

        variable_ids_to_delete = [row[0] for row in rows]

        # nothing to delete, quit
        if not variable_ids_to_delete:
            return True

        log.info("cleanup_ghost_variables.start", size=len(variable_ids_to_delete))

        # raise an exception if they're used in any charts
        rows = con.execute(
            text(
                """
            SELECT chartId, variableId FROM chart_dimensions WHERE variableId IN :variable_ids
        """
            ),
            {"dataset_id": dataset_id, "variable_ids": variable_ids_to_delete},
        ).fetchall()
        if rows:
            rows = pd.DataFrame(rows, columns=["chartId", "variableId"])

            # show a warning
            log.warning(
                "Variables used in charts will not be deleted automatically",
                rows=rows,
                variables=variable_ids_to_delete,
            )
            return False

        # then variables themselves with related data in other tables
        con.execute(
            text(
                """
            DELETE FROM country_latest_data WHERE variable_id IN :variable_ids
        """
            ),
            {"variable_ids": variable_ids_to_delete},
        )

        # delete relationships
        con.execute(
            text(
                """
            DELETE FROM origins_variables WHERE variableId IN :variable_ids
        """
            ),
            {"variable_ids": variable_ids_to_delete},
        )
        con.execute(
            text(
                """
            DELETE FROM tags_variables_topic_tags WHERE variableId IN :variable_ids
        """
            ),
            {"variable_ids": variable_ids_to_delete},
        )
        con.execute(
            text(
                """
            DELETE FROM posts_gdocs_variables_faqs WHERE variableId IN :variable_ids
        """
            ),
            {"variable_ids": variable_ids_to_delete},
        )

        # delete them from explorers
        con.execute(
            text(
                """
            DELETE FROM explorer_variables WHERE variableId IN :variable_ids
        """
            ),
            {"variable_ids": variable_ids_to_delete},
        )

        # finally delete variables
        result = con.execute(
            text(
                """
            DELETE FROM variables WHERE datasetId = :dataset_id AND id IN :variable_ids
        """
            ),
            {"dataset_id": dataset_id, "variable_ids": variable_ids_to_delete},
        )

        con.commit()

        log.warning(
            "cleanup_ghost_variables.end",
            size=result.rowcount,
            variables=variable_ids_to_delete,
        )

        return True


def cleanup_ghost_sources(engine: Engine, dataset_id: int, upserted_source_ids: List[int]) -> None:
    """Remove all leftover sources that didn't get upserted into DB during grapher step.
    This could happen when you rename or delete sources.
    :param dataset_id: ID of the dataset
    :param upserted_source_ids: sources upserted in grapher step
    """
    with engine.connect() as con:
        if upserted_source_ids:
            result = con.execute(
                text("""DELETE FROM sources WHERE datasetId = :dataset_id AND id NOT IN :source_ids"""),
                {"dataset_id": dataset_id, "source_ids": upserted_source_ids},
            )
        else:
            result = con.execute(
                text("""DELETE FROM sources WHERE datasetId = :dataset_id"""),
                {"dataset_id": dataset_id},
            )
        if result.rowcount > 0:
            con.commit()
            log.warning(f"Deleted {result.rowcount} ghost sources")


def _get_entity_name(session: Session, entity_id: int) -> str:
    q = select(gm.Entity).where(gm.Entity.id == entity_id)
    entity = session.scalars(q).one_or_none()
    return entity.name if entity else ""

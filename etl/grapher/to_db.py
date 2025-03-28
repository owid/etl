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
from owid.catalog import Table, VariableMeta, utils
from owid.catalog.utils import hash_any
from sqlalchemy import select, text, update
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from apps.backport.datasync import data_metadata as dm
from apps.backport.datasync.datasync import upload_gzip_string
from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffsLoader
from etl import config
from etl.db import get_engine, production_or_master_engine
from etl.grapher import helpers as gh

from . import model as gm

log = structlog.get_logger()


source_table_lock = Lock()
origins_table_lock = Lock()


CURRENT_DIR = os.path.dirname(__file__)


@dataclass
class DatasetUpsertResult:
    dataset_id: int
    source_ids: Dict[int, int]


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
            dataset.metadata,
            namespace=namespace,
            user_id=int(cast(str, config.GRAPHER_USER_ID)),
            table_names=dataset.table_names,
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
            url=f"http://{engine.url.host}/admin/datasets/{ds.id}",
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

        # Templates can make numDecimalPlaces string, convert it to int
        if meta.display and isinstance(meta.display.get("numDecimalPlaces"), str):
            meta.display["numDecimalPlaces"] = int(meta.display["numDecimalPlaces"])

        # Prune empty fields from description_key
        if meta.description_key:
            meta.description_key = [k for k in meta.description_key if k.strip()]


def _check_upserted_table(table: Table) -> None:
    assert set(table.index.names) == {"year", "entityId", "entityCode", "entityName"}, (
        "Tables to be upserted must have 4 indices: year, entityId, entityCode, entityName. Instead"
        f" they have: {table.index.names}"
    )
    assert len(table.columns) == 1, (
        "Tables to be upserted must have only 1 column. Instead they have:" f" {table.columns.names}"
    )
    assert table[table.columns[0]].title, f"Column `{table.columns[0]}` must have a title in metadata"
    assert table.iloc[:, 0].notnull().all(), (
        "Tables to be upserted must have no null values. Instead they" f" have:\n{table.loc[table.iloc[:, 0].isnull()]}"
    )
    utils.validate_underscore(table.metadata.short_name, "Table's short_name")
    utils.validate_underscore(table.columns[0], "Variable's name")

    # make sure we have unique (year, entity_id) pairs
    pairs = table.index.get_level_values("entityName").astype(str) + table.index.get_level_values("year").astype(str)
    vc = pairs.value_counts()
    if (vc > 1).any():
        duplicates = vc[vc > 1].index.tolist()
        raise AssertionError(f"Duplicates (entityName, year):\n {duplicates}")

    if len(table.iloc[:, 0].metadata.sources) > 1:
        raise NotImplementedError(
            "only a single source is supported for grapher datasets, use"
            " `combine_metadata_sources` or `adapt_dataset_metadata_for_grapher` to"
            " join multiple sources"
        )

    assert not gh.contains_inf(table.iloc[:, 0]), f"Column `{table.columns[0]}` has inf values"


def upsert_table(
    engine: Engine,
    admin_api: AdminAPI,
    table: Table,
    dataset_upsert_result: DatasetUpsertResult,
    catalog_path: str,
    dimensions: Optional[gm.Dimensions] = None,
    verbose: bool = True,
) -> None:
    """This function is used to put one ready to go formatted Table (i.e.
    in the format (year, entityId, value)) into mysql. The metadata
    of the variable is used to fill the required fields.
    """

    # We sometimes get a warning, but it's unclear where it is coming from
    # Passing a BlockManager to Table is deprecated and will raise in a future version. Use public APIs instead.
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    _check_upserted_table(table)

    _update_variables_metadata(table)

    # For easy retrieveal of the value series we store the name
    column_name = table.columns[0]
    variable_meta: VariableMeta = table[column_name].metadata

    # All following functions assume that `value` is string
    # NOTE: we could make the code more efficient if we didn't convert `value` to string
    # TODO: can we avoid setting & resetting index back and forth?
    df = table.reset_index().rename(columns={column_name: "value"})
    df["value"] = df["value"].astype("string")

    checksum_data = calculate_checksum_data(df)
    checksum_metadata = calculate_checksum_metadata(variable_meta, df)

    with Session(engine) as session:
        # compare checksums
        try:
            db_variable = gm.Variable.from_catalog_path(session, catalog_path)
        except NoResultFound:
            db_variable = None

        upsert_metadata_kwargs = dict(
            session=session,
            df=df,
            variable_meta=variable_meta,
            column_name=column_name,
            dataset_upsert_result=dataset_upsert_result,
            catalog_path=catalog_path,
            dimensions=dimensions,
            admin_api=admin_api,
        )

        # create variable if it doesn't exist
        if not db_variable:
            db_variable = upsert_metadata(**upsert_metadata_kwargs)
            upsert_data(df, db_variable.s3_data_path())

        # variable exists
        else:
            if (
                not config.FORCE_UPLOAD
                and db_variable.dataChecksum == checksum_data
                and db_variable.metadataChecksum == checksum_metadata
            ):
                if verbose:
                    log.info("upsert_table.skipped_no_changes", size=len(df), variable_id=db_variable.id)
                return

            # NOTE: sequantial upserts are slower than parallel, but they will be useful once we switch to asyncio
            # if db_variable.dataChecksum != checksum_data:
            #     upsert_data(df, db_variable.s3_data_path())
            # if db_variable.metadataChecksum != checksum_metadata:
            #     db_variable = upsert_metadata(**upsert_metadata_kwargs)

            futures = {}
            with ThreadPoolExecutor() as executor:
                if config.FORCE_UPLOAD or db_variable.dataChecksum != checksum_data:
                    futures["data"] = executor.submit(upsert_data, df, db_variable.s3_data_path())

                if config.FORCE_UPLOAD or db_variable.metadataChecksum != checksum_metadata:
                    futures["metadata"] = executor.submit(upsert_metadata, **upsert_metadata_kwargs)

            if futures:
                # Wait for futures to complete in case exceptions are raised
                if "data" in futures:
                    futures["data"].result()
                if "metadata" in futures:
                    db_variable = futures["metadata"].result()

        # Update checksums
        db_variable.dataChecksum = checksum_data
        db_variable.metadataChecksum = checksum_metadata

        # Commit new checksums
        session.add(db_variable)
        session.commit()

        if verbose:
            log.info("upsert_table.uploaded_to_s3", size=len(df), variable_id=db_variable.id)


def upsert_data(df: pd.DataFrame, s3_data_path: str):
    # upload data to R2
    var_data = dm.variable_data(df)
    var_data_str = json.dumps(var_data, default=str)
    upload_gzip_string(var_data_str, s3_data_path)


def upsert_metadata(
    session: Session,
    df: pd.DataFrame,
    variable_meta: VariableMeta,
    column_name: str,
    dataset_upsert_result: DatasetUpsertResult,
    catalog_path: str,
    dimensions: Optional[gm.Dimensions],
    admin_api: AdminAPI,
) -> gm.Variable:
    timespan = _get_timespan(df, variable_meta)

    source_id = _add_or_update_source(session, variable_meta, column_name, dataset_upsert_result)

    with origins_table_lock:
        db_origins = _add_or_update_origins(session, variable_meta.origins)
        # commit within the lock to make sure other threads get the latest sources
        session.commit()

    # pop grapher_config from variable metadata, later we send it to Admin API
    if variable_meta.presentation and variable_meta.presentation.grapher_config:
        grapher_config = variable_meta.presentation.grapher_config
        variable_meta.presentation.grapher_config = None
    else:
        grapher_config = None

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

    # TODO: `type` is part of metadata, but not part of checksum!
    if not db_variable.type:
        db_variable.type = db_variable.infer_type(df["value"])

    # update links, we need to do it after we commit deleted relationships above
    db_variable.update_links(
        session,
        db_origins,
        faqs=variable_meta.presentation.faqs if variable_meta.presentation else [],
        tag_names=variable_meta.presentation.topic_tags if variable_meta.presentation else [],
    )
    session.add(db_variable)

    # we need to commit changes because `dm.variable_metadata` pulls all data from MySQL
    # and sends it to R2
    # NOTE: we could optimize this by evading pulling from MySQL and instead constructing JSON files from objects
    #   we have available
    session.commit()

    # grapher_config needs to be sent to Admin API because it has side effects
    if grapher_config:
        admin_api.put_grapher_config(db_variable_id, grapher_config)
    # grapher_config does not exist, but it's still in the database -> delete it
    elif not grapher_config and db_variable.grapherConfigIdETL:
        admin_api.delete_grapher_config(db_variable_id)

    # upload metadata to R2
    var_metadata = dm.variable_metadata(session, db_variable.id, df)
    var_metadata_str = json.dumps(var_metadata, default=str)

    # upload them to R2
    upload_gzip_string(var_metadata_str, db_variable.s3_metadata_path())

    return db_variable


def calculate_checksum_metadata(variable_meta: VariableMeta, df: pd.DataFrame) -> str:
    # entities and years are also part of the metadata checksum
    return str(
        hash_any(
            (
                hash_any(sorted(df.entityId.unique())),
                hash_any(sorted(df.year.unique())),
                hash_any(variable_meta),
            )
        )
    )


def calculate_checksum_data(df: pd.DataFrame) -> str:
    # checksum that is invariant to sorting or index reset
    return str(pd.util.hash_pandas_object(df).sum())


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
            gm.Dataset.catalogPath
            == f"{dataset.metadata.namespace}/{dataset.metadata.version}/{dataset.metadata.short_name}"
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

            if _raise_error_for_deleted_variables(rows):
                raise ValueError(f"Variables used in charts will not be deleted automatically:\n{rows}")
            else:
                # otherwise show a warning
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

        # NOTE: deleting mdim variables form grapher:// step could be a bit unexpected and could
        # lead to side effects. If this causes problems, we should clean up ghost variables after
        # the entire ETL & chart-sync is finished.
        # delete from dependent multi_dim_x_chart_configs to avoid foreign key constraint
        con.execute(
            text(
                """
            DELETE FROM multi_dim_x_chart_configs WHERE variableId IN :variable_ids
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


def _raise_error_for_deleted_variables(rows: pd.DataFrame) -> bool:
    """If we run into ghost variables that are still used in charts, should we raise an error?"""
    # raise an error if on staging server
    if config.ENV == "staging":
        # It's possible that we merged changes to ETL, but the staging server still uses old charts. In
        # that case, we first check that the charts were really modified on our staging server.
        modified_charts = ChartDiffsLoader(config.OWID_ENV.get_engine(), production_or_master_engine()).df
        return bool(set(modified_charts.index) & set(rows.chartId))
    # Only show a warning in production. We can't raise an error because if someone merges changes to ETL
    # with renamed variables and valid chart-sync, the ETL deploy would fail. It would fail because ETL (and this part) runs
    # before chart-sync. If we only show a warning, the function `cleanup_ghost_variables` returns False, and ETL will
    # re-run the step on the next deploy and delete those ghost variables.
    # See https://github.com/owid/etl/issues/4099 for more details.
    elif config.ENV == "production":
        return False
    # always raise an error otherwise
    else:
        return True


def cleanup_ghost_sources(engine: Engine, dataset_id: int, dataset_upserted_source_ids: List[int]) -> None:
    """Remove all leftover sources that didn't get upserted into DB during grapher step.
    This could happen when you rename or delete sources.
    :param dataset_id: ID of the dataset
    :param dataset_upserted_source_ids: upserted dataset sources, we combine them with variable sources
    """
    with engine.connect() as con:
        where = " AND id NOT IN :dataset_source_ids" if dataset_upserted_source_ids else ""

        result = con.execute(
            text(
                f"""
            DELETE FROM sources
            WHERE datasetId = :dataset_id
                AND id NOT IN (
                    select distinct sourceId from variables where datasetId = :dataset_id
                )
                {where}
                """
            ),
            {"dataset_id": dataset_id, "dataset_source_ids": dataset_upserted_source_ids},
        )
        if result.rowcount > 0:
            con.commit()
            log.warning(f"Deleted {result.rowcount} ghost sources")


def _get_timespan(table: pd.DataFrame, variable_meta: VariableMeta) -> str:
    # Timespan does not work for yearIsDay variables
    if (variable_meta.display or {}).get("yearIsDay"):
        return ""
    else:
        years = table.year.unique()
        if len(years) == 0:
            return ""
        else:
            min_year = min(years)
            max_year = max(years)
            return f"{min_year}-{max_year}"

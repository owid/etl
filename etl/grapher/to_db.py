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
from dataclasses import dataclass
from typing import Any, cast

import pandas as pd
import requests
import structlog
from owid import catalog
from owid.catalog import Table, Variable, VariableMeta, utils
from owid.catalog.core.meta import update_variable_metadata
from owid.catalog.core.utils import hash_any
from sqlalchemy import select, update
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

from apps.backport.datasync import data_metadata as dm
from apps.backport.datasync.datasync import upload_gzip_string
from apps.chart_sync.admin_api import AdminAPI
from etl import config
from etl.db import get_engine, production_or_master_engine, read_sql
from etl.grapher import helpers as gh

from . import model as gm

log = structlog.get_logger()


CURRENT_DIR = os.path.dirname(__file__)


@dataclass
class DatasetUpsertResult:
    dataset_id: int
    # Dataset-level fields that end up in every indicator's metadata JSON (see
    # `_dataset_metadata_fields`). They take part in the metadata checksum, so that editing one
    # of them re-uploads the affected files.
    metadata_fields: dict[str, Any]


def _dataset_metadata_fields(ds: gm.Dataset) -> dict[str, Any]:
    """Dataset-level fields that `_load_variable` joins into every indicator's metadata JSON.

    Kept in sync with the SELECT in `apps/backport/datasync/data_metadata.py`. `sourceName` and
    `sourceDescription` are deliberately left out: ETL upserts variables with `sourceId=None`, so
    the legacy `sources` join only ever contributes to backported datasets.

    None values are dropped, mirroring `_omit_nullable_values` before upload - a field that is
    null (and therefore absent from the JSON) must not take part in the hash, otherwise adding a
    field here would flip the checksum of every variable that doesn't set it.
    """
    fields = {
        "datasetName": ds.name,
        "datasetVersion": ds.version,
        "updatePeriodDays": ds.updatePeriodDays,
        "nonRedistributable": bool(ds.nonRedistributable),
    }
    return {k: v for k, v in fields.items() if v is not None}


def upsert_dataset(engine: Engine, dataset: catalog.Dataset, namespace: str) -> DatasetUpsertResult:
    assert dataset.metadata.short_name, "Dataset must have a short_name"
    assert dataset.metadata.version, "Dataset must have a version"
    assert dataset.metadata.title, "Dataset must have a title"

    utils.validate_underscore(dataset.metadata.short_name, "Dataset's short_name")

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

        session.commit()

        return DatasetUpsertResult(ds.id, _dataset_metadata_fields(ds))


def check_table(table: Table) -> None:
    assert set(table.index.names) >= {"year", "entityId", "entityCode", "entityName"}, (
        "Table to be upserted must have those 4 indices: year, entityId, entityCode, entityName. Instead"
        f" they have: {table.index.names}"
    )

    for col in table.columns:
        assert table[col].title, f"Column `{col}` must have a title in metadata"

        origins = table[col].m.origins
        assert origins, f"Column `{col}` must have at least one origin"
        assert len(origins) == len(set(origins)), "origins must be unique"

    utils.validate_underscore(table.metadata.short_name, "Table's short_name")
    utils.validate_underscore(table.columns[0], "Variable's name")

    # make sure we have unique (year, entity_id) pairs
    if not table.index.is_unique:
        pairs = table.index.get_level_values("entityName").astype(str)
        index_names = ["entityName"]
        for col in table.index.names:
            if col in ("entityCode", "entityId", "entityName"):
                continue
            pairs += ", " + table.index.get_level_values(col).astype(str)
            index_names.append(col)

        vc = pairs.value_counts()
        if (vc > 1).any():
            duplicates = vc[vc > 1].index.tolist()
            raise AssertionError(f"Duplicates ({', '.join(index_names)}):\n {duplicates}")


def _check_upserted_variable(variable: Variable) -> None:
    assert variable.notnull().all(), (
        f"Tables to be upserted must have no null values. Instead they have:\n{variable.loc[variable.isnull()]}"
    )
    assert not gh.contains_inf(variable), f"Column `{variable.name}` has inf values"


def load_dataset_variables(dataset_id: int, engine: Engine) -> dict[int | str, Any]:
    q = """
    select catalogPath, id, dataChecksum, metadataChecksum from variables where datasetId = %(dataset_id)s
    """
    return (
        read_sql(q, engine=engine, params={"dataset_id": dataset_id}).set_index("catalogPath").to_dict(orient="index")  # ty: ignore[invalid-return-type]
    )


@dataclass
class PreparedVariable:
    """A variable ready to be sent to the Admin API, with what ETL needs afterwards."""

    catalog_path: str
    payload: dict[str, Any]
    df: pd.DataFrame
    checksum_data: str
    checksum_metadata: str
    data_changed: bool
    grapher_config: dict[str, Any] | None
    payload_bytes: int


def prepare_variable(
    table: Table,
    dataset_upsert_result: DatasetUpsertResult,
    catalog_path: str,
    checksums: dict,
    dimensions: gm.Dimensions | None = None,
    verbose: bool = True,
) -> PreparedVariable | None:
    """Turn one single-column Table into an Admin API payload, or None if nothing changed.

    Only the parts that need the values happen here: the checksums, the inferred `type`, the
    timespan, and the distinct entities and years the published JSON lists. Everything else is
    metadata, and Grapher does it.
    """
    # We sometimes get a warning, but it's unclear where it is coming from
    # Passing a BlockManager to Table is deprecated and will raise in a future version. Use public APIs instead.
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    _check_upserted_variable(table.iloc[:, 0])

    # For easy retrieveal of the value series we store the name
    column_name = table.columns[0]
    variable_meta: VariableMeta = table[column_name].metadata

    variable_meta = update_variable_metadata(variable_meta)

    # All following functions assume that `value` is string
    # NOTE: we could make the code more efficient if we didn't convert `value` to string
    # TODO: can we avoid setting & resetting index back and forth?
    df = table.reset_index().rename(columns={column_name: "value"})
    df["value"] = df["value"].astype("string")

    checksum_data = calculate_checksum_data(df)
    checksum_metadata = calculate_checksum_metadata(variable_meta, df, dataset_upsert_result.metadata_fields)

    if config.FORCE_UPLOAD:
        checksums["dataChecksum"] = None
        checksums["metadataChecksum"] = None

    # Both checksums match
    if checksums.get("dataChecksum") == checksum_data and checksums.get("metadataChecksum") == checksum_metadata:
        if verbose:
            log.debug("upsert_table.skipped_no_changes", size=len(df), catalog_path=catalog_path)
        return None

    # grapher_config has side effects on inheriting charts, so it keeps going through its own
    # Admin API call rather than riding along in the variable payload.
    grapher_config = None
    if variable_meta.presentation and variable_meta.presentation.grapher_config:
        grapher_config = variable_meta.presentation.grapher_config
        variable_meta.presentation.grapher_config = None

    payload = _variable_payload(
        variable_meta=variable_meta,
        df=df,
        column_name=column_name,
        catalog_path=catalog_path,
        dimensions=dimensions,
    )

    return PreparedVariable(
        catalog_path=catalog_path,
        payload=payload,
        df=df,
        checksum_data=checksum_data,
        checksum_metadata=checksum_metadata,
        data_changed=checksums.get("dataChecksum") != checksum_data,
        grapher_config=grapher_config,
        payload_bytes=len(json.dumps(payload, default=str)),
    )


def _variable_payload(
    variable_meta: VariableMeta,
    df: pd.DataFrame,
    column_name: str,
    catalog_path: str,
    dimensions: gm.Dimensions | None,
) -> dict[str, Any]:
    """The wire format of one variable.

    Entities go as ids only — Grapher resolves names and codes from its own `entities` table.
    `descriptionKey` goes as a markdown string; Grapher never sees the legacy list form.
    """
    assert variable_meta.origins, f"Variable `{column_name}` must have at least one origin"

    presentation = variable_meta.presentation
    description_key = variable_meta.description_key
    if description_key is not None:
        assert isinstance(description_key, str), "descriptionKey should be a markdown string"

    return {
        "catalogPath": catalog_path,
        "shortName": column_name,
        "name": variable_meta.title,
        "unit": variable_meta.unit,
        "shortUnit": variable_meta.short_unit,
        "description": variable_meta.description,
        "descriptionShort": variable_meta.description_short,
        "descriptionFromProducer": variable_meta.description_from_producer,
        "descriptionKey": description_key,
        "descriptionProcessing": variable_meta.description_processing,
        "titlePublic": presentation.title_public if presentation else None,
        "titleVariant": presentation.title_variant if presentation else None,
        "attribution": presentation.attribution if presentation else None,
        "attributionShort": presentation.attribution_short if presentation else None,
        "coverage": "",
        "timespan": _get_timespan(df, variable_meta),
        "display": variable_meta.display or {},
        "dimensions": dimensions,
        "schemaVersion": variable_meta.schema_version,
        "processingLevel": variable_meta.processing_level,
        "license": variable_meta.license.to_dict() if variable_meta.license else None,
        "licenses": [license.to_dict() for license in variable_meta.licenses] if variable_meta.licenses else None,
        "sort": variable_meta.sort,
        # Inferred from the values, which Grapher never receives.
        "type": variable_meta.type or gm.Variable.infer_type(df["value"]),
        "origins": [_origin_payload(origin) for origin in variable_meta.origins],
        "topicTags": presentation.topic_tags if presentation and presentation.topic_tags else [],
        "faqs": [{"gdocId": faq.gdoc_id, "fragmentId": faq.fragment_id} for faq in presentation.faqs]
        if presentation and presentation.faqs
        else [],
        "entityIds": sorted(df["entityId"].unique().tolist()),
        "years": sorted(df["year"].unique().tolist()),
    }


def _origin_payload(origin: catalog.Origin) -> dict[str, Any]:
    return {
        "title": origin.title,
        "titleSnapshot": origin.title_snapshot,
        "description": origin.description,
        "descriptionSnapshot": origin.description_snapshot,
        "producer": origin.producer,
        "citationFull": origin.citation_full,
        "attribution": origin.attribution,
        "attributionShort": origin.attribution_short,
        "versionProducer": origin.version_producer,
        "urlMain": origin.url_main,
        "urlDownload": origin.url_download,
        "dateAccessed": str(origin.date_accessed) if origin.date_accessed else None,
        "datePublished": origin.date_published,
        "license": origin.license.to_dict() if origin.license else None,
    }


def _s3_data_path(variable_id: int) -> str:
    return f"{config.BAKED_VARIABLES_PATH}/{variable_id}.data.json"


def _s3_metadata_path(variable_id: int) -> str:
    return f"{config.BAKED_VARIABLES_PATH}/{variable_id}.metadata.json"


def upload_data(df: pd.DataFrame, s3_data_path: str) -> None:
    # upload data to R2
    var_data = dm.variable_data(df)
    var_data_str = json.dumps(var_data, default=str)
    upload_gzip_string(var_data_str, s3_data_path)


def flush_variable_batch(
    engine: Engine,
    admin_api: AdminAPI,
    dataset_id: int,
    batch: list[PreparedVariable],
    verbose: bool = True,
) -> None:
    """Send a chunk of variables to the Admin API, then publish their files.

    Order matters and mirrors what this did as direct SQL: MySQL first, then R2, and only then
    the checksums — so a failure anywhere leaves the variable to be redone rather than recorded
    as published.
    """
    if not batch:
        return

    response = admin_api.upsert_variables(dataset_id, [prepared.payload for prepared in batch])
    upserted = response["variables"]

    checksums = {}
    for prepared in batch:
        result = upserted[prepared.catalog_path]
        variable_id = result["id"]

        # Grapher assembled this from the rows it just wrote; we only publish it.
        upload_gzip_string(
            json.dumps(result["metadata"], default=str),
            _s3_metadata_path(variable_id),
        )

        if prepared.data_changed:
            upload_data(prepared.df, _s3_data_path(variable_id))

        if prepared.grapher_config:
            admin_api.put_grapher_config(variable_id, prepared.grapher_config)

        checksums[prepared.catalog_path] = {
            "dataChecksum": prepared.checksum_data,
            "metadataChecksum": prepared.checksum_metadata,
        }

    admin_api.set_variable_checksums(dataset_id, checksums)

    if verbose:
        log.info("upsert_table.uploaded_to_s3", size=len(batch))


def calculate_checksum_metadata(variable_meta: VariableMeta, df: pd.DataFrame, dataset_metadata: dict[str, Any]) -> str:
    # Hash the canonical (pruned) dict representation, not the dataclass itself.
    # Dataclass-shape changes (a field renamed, added, or removed — even when the
    # default is None / [] and the JSON output is unchanged) used to spuriously
    # flip the checksum and lit up chart-diff as METADATA CHANGE with an empty
    # UI diff. `to_dict()` goes through `@pruned_json`, which drops None/empty
    # values the same way `_omit_nullable_values` does before upload to S3 — so
    # the checksum now matches what the comparator actually sees.
    #
    # Performance: `to_dict()` (via DataClassJsonMixin) is ~130µs/call on a
    # realistic VariableMeta — ~3× slower than hashing the raw dataclass. Per
    # upsert this is drowned out by the S3+MySQL roundtrips (100s of ms each).
    # If this ever shows up in a profile, replace with a custom field walker
    # that prunes inline, same trick `dataclass_from_dict` uses in core/utils.py.
    #
    # entities and years are also part of the metadata checksum.
    #
    # So are the dataset-level fields the JSON embeds (`_dataset_metadata_fields`): they live on
    # the dataset, not on the variable, so without them clearing e.g. `update_period_days` left
    # every variable's checksum untouched, every upload was skipped, and the file in R2 stayed
    # stale forever while MySQL was correct.
    return str(
        hash_any(
            (
                hash_any(sorted(df.entityId.unique())),
                hash_any(sorted(df.year.unique())),
                hash_any(variable_meta.to_dict()),
                hash_any(dataset_metadata),
            )
        )
    )


def calculate_checksum_data(df: pd.DataFrame) -> str:
    # checksum that is invariant to sorting or index reset
    return str(pd.util.hash_pandas_object(df).sum())


def fetch_db_checksum(dataset: catalog.Dataset) -> str | None:
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
            .where(gm.Dataset.id == dataset_id)  # ty: ignore
            .values(
                sourceChecksum=checksum,
                dataEditedAt=datetime.datetime.now(datetime.timezone.utc),
                metadataEditedAt=datetime.datetime.now(datetime.timezone.utc),
            )
        )
        session.execute(q)
        session.commit()


def cleanup_ghost_variables(admin_api: AdminAPI, dataset_id: int, upserted_variable_ids: list[int]) -> bool:
    """Remove all leftover variables that didn't get upserted into DB during grapher step.
    This could happen when you rename or delete a variable in ETL.
    Raise an error if we try to delete variable used by any chart.

    The delete itself is done by the Grapher admin API, which owns the tables that hang off
    `variables` and the chart configs a variable leaves behind in MySQL and R2. It deletes
    what it safely can and hands back the variables a chart still uses; deciding whether
    those should fail the run is ours.

    :param admin_api: Grapher admin API client
    :param dataset_id: ID of the dataset
    :param upserted_variable_ids: variables upserted in grapher step

    :return: True if successful
    """
    try:
        result = admin_api.cleanup_ghost_variables(dataset_id, upserted_variable_ids)
    except requests.exceptions.ConnectionError:
        # Deployed environments always have an admin server, so failing to reach one there is
        # an outage, not a workflow: let it fail rather than quietly skipping cleanup.
        if config.ENV in ("staging", "production"):
            raise
        # Working locally without a running Grapher admin. Leaving the ghost variables behind
        # is harmless there, so warn instead of failing the step — but report the cleanup as
        # unsuccessful, so the checksum stays unset and a later run against a reachable admin
        # picks them up rather than recording a sweep that never happened.
        log.warning(
            "cleanup_ghost_variables.admin_api_unreachable",
            admin_api=admin_api.owid_env.admin_api,
            dataset_id=dataset_id,
        )
        return False

    if result["deleted"]:
        log.warning(
            "cleanup_ghost_variables.end",
            size=len(result["deleted"]),
            variables=result["deleted"],
        )

    if not result["blocked"]:
        return True

    rows = pd.DataFrame(result["blocked"], columns=["variableId", "variableName", "chartId", "chartSlug"])

    message = "Variables used in charts will not be deleted automatically. Ignore this if your PR doesn't affect the problematic variables."

    if _raise_error_for_deleted_variables(rows):
        raise ValueError(f"{message}:\n{rows}")

    # otherwise show a warning
    log.warning(message, rows=rows)
    return False


def _raise_error_for_deleted_variables(rows: pd.DataFrame) -> bool:
    """If we run into ghost variables that are still used in charts, should we raise an error?"""
    # raise an error if on staging server
    if config.ENV == "staging":
        # It's possible that we merged changes to ETL, but the staging server still uses old charts. In
        # that case, we first check that the charts were really modified on our staging server.

        # Load this dynamically for performance reasons
        from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffsLoader

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


def _get_timespan(table: pd.DataFrame, variable_meta: VariableMeta) -> str:
    display = variable_meta.display or {}

    # Timespan does not work for sub-yearly data.
    if display.get("timeInterval") in {"day", "week", "month", "quarter"}:
        return ""

    years = table.year.unique()
    if len(years) == 0:
        return ""

    min_year = min(years)
    max_year = max(years)
    if display.get("timeInterval") == "decade":
        # Each value codes a calendar decade (e.g. 1820s = 1820–1829) by a representative year
        # within it; snap the start down and the end up to the decade boundaries so the timespan
        # reflects the full coverage (e.g. 1820–2019, not 1820–2010).
        min_year = (min_year // 10) * 10
        max_year = (max_year // 10) * 10 + 9
    return f"{min_year}-{max_year}"

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
import structlog
from owid import catalog
from owid.catalog import Table, Variable, VariableMeta, utils
from owid.catalog.core.meta import update_variable_metadata
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
class PreparedIndicator:
    """One indicator's payload, plus what ETL needs after the Admin API replies."""

    catalog_path: str
    payload: dict[str, Any]
    df: pd.DataFrame
    checksum_data: str
    payload_bytes: int


def prepare_indicator(
    table: Table,
    catalog_path: str,
    dimensions: gm.Dimensions | None = None,
) -> PreparedIndicator:
    """Turn one single-column Table into an Admin API payload.

    Metadata is always sent — Grapher compares it against what it published last time, which
    is where the previous version actually lives. Only the parts that need the values happen
    here: the data checksum, the inferred `type`, the timespan, and the distinct entities and
    years the published JSON lists.
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

    payload = _indicator_payload(
        variable_meta=variable_meta,
        df=df,
        column_name=column_name,
        catalog_path=catalog_path,
        dimensions=dimensions,
        checksum_data=checksum_data,
    )

    return PreparedIndicator(
        catalog_path=catalog_path,
        payload=payload,
        df=df,
        checksum_data=checksum_data,
        payload_bytes=len(json.dumps(payload, default=str)),
    )


def _indicator_payload(
    variable_meta: VariableMeta,
    df: pd.DataFrame,
    column_name: str,
    catalog_path: str,
    dimensions: gm.Dimensions | None,
    checksum_data: str,
) -> dict[str, Any]:
    """The wire format of one indicator.

    Entities go as ids only — Grapher resolves names and codes from its own `entities` table.
    `descriptionKey` goes as a markdown string; Grapher never sees the legacy list form.
    """
    assert variable_meta.origins, f"Variable `{column_name}` must have at least one origin"

    presentation = variable_meta.presentation
    description_key = variable_meta.description_key
    if description_key is not None:
        assert isinstance(description_key, str), "descriptionKey should be a markdown string"

    grapher_config = presentation.grapher_config if presentation else None

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
        "grapherConfig": grapher_config,
        # Inferred from the values, which Grapher never receives.
        "type": variable_meta.type or gm.Variable.infer_type(df["value"]),
        "origins": [_origin_payload(origin) for origin in variable_meta.origins],
        "topicTags": presentation.topic_tags if presentation and presentation.topic_tags else [],
        "faqs": [{"gdocId": faq.gdoc_id, "fragmentId": faq.fragment_id} for faq in presentation.faqs]
        if presentation and presentation.faqs
        else [],
        "entityIds": sorted(df["entityId"].unique().tolist()),
        "years": sorted(df["year"].unique().tolist()),
        "dataChecksum": checksum_data,
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


def upload_data(df: pd.DataFrame, s3_data_path: str) -> None:
    # upload data to R2
    var_data = dm.variable_data(df)
    var_data_str = json.dumps(var_data, default=str)
    upload_gzip_string(var_data_str, s3_data_path)


def flush_indicator_batch(
    admin_api: AdminAPI,
    dataset_catalog_path: str,
    batch: list[PreparedIndicator],
    verbose: bool = True,
) -> dict[str, str]:
    """Send a chunk of indicators, then upload the data files Grapher asked for.

    Returns the data checksums of the files actually published, for the caller to report once
    the whole run has finished. Nothing here records a checksum: an upload that succeeded but
    was never reported just gets redone, whereas one reported before it happened would leave a
    stale file looking current.
    """
    if not batch:
        return {}

    response = admin_api.put_indicators(dataset_catalog_path, [prepared.payload for prepared in batch])
    indicators = response["indicators"]

    published = {}
    for prepared in batch:
        result = indicators[prepared.catalog_path]
        if result["uploadData"]:
            upload_data(prepared.df, result["dataPath"])
        published[prepared.catalog_path] = prepared.checksum_data

    if verbose:
        log.info("upsert_table.uploaded_to_s3", size=len(batch))

    return published


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


def blocked_indicators_allow_run(blocked: list[dict[str, Any]]) -> bool:
    """Decide what to do about indicators Grapher wouldn't remove because a chart still uses
    them. Returns True if the run should go ahead.

    Grapher reports; we decide. The rule depends on which environment we're in and, on staging,
    on a chart-diff against production — neither of which a staging admin server can work out.
    """
    if not blocked:
        return True

    rows = pd.DataFrame(
        [
            {"catalogPath": entry["catalogPath"], "chartId": chart["id"], "chartSlug": chart["slug"]}
            for entry in blocked
            for chart in entry["charts"]
        ],
        columns=["catalogPath", "chartId", "chartSlug"],
    )

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

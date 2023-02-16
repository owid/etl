import concurrent.futures
import datetime as dt
import gzip
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import rich_click as click
import structlog
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError
from dataclasses_json import dataclass_json
from sqlalchemy.engine import Engine
from sqlmodel import Session
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from etl import config
from etl import grapher_model as gm
from etl.db import get_engine
from etl.publish import connect_s3

from .data_metadata import (
    variable_data,
    variable_data_df_from_mysql,
    variable_data_df_from_s3,
    variable_metadata,
)

log = structlog.get_logger()

config.enable_bugsnag()


S3_BUCKET = "owid-catalog"
S3_PREFIX = f"baked-variables/{config.DB_NAME}"

# bucket owid-catalog is behind Cloudflare
if S3_BUCKET == "owid-catalog":
    S3_ENDPOINT = "https://catalog.ourworldindata.org"
else:
    S3_ENDPOINT = f"https://{S3_BUCKET}.nyc3.digitaloceanspaces.com"


@click.command(help=__doc__)
@click.option("--dataset-ids", "-d", type=int, multiple=True)
@click.option(
    "--dt-start",
    type=click.DateTime(),
    help="Datetime to start updating datasets from",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not add dataset to a catalog on dry-run",
)
@click.option(
    "--force/--no-force",
    default=False,
    type=bool,
    help="Force overwrite even if checksums match",
)
@click.option(
    "--workers",
    type=int,
    help="Thread workers to parallelize which steps need rebuilding (steps execution is not parallelized)",
    default=1,
)
def cli(
    dataset_ids: tuple[int],
    dt_start: dt.datetime,
    dry_run: bool,
    force: bool,
    workers: int,
) -> None:
    """TBD

    Usage:
        etl-datasync ...

        # sync data since some date
        ENV=.env.staging backport-datasync --dt-start "2023-01-01 00:00:00" --workers 10

        # real-time sync in a forever loop
        last_dt="2023-01-01 00:00:00"
        while true; do
            new_dt=$(date +"%Y-%m-%d %H:%M:%S")
            ENV=.env.staging backport-datasync --workers 10 --dt-start "$last_dt"
            last_dt=$new_dt
            sleep 5
        done;

    To fill `dataPath` column for all datasets from live_grapher DB run the following SQL:

    ```sql
    update variables set dataPath = CONCAT('https://catalog.ourworldindata.org/baked-variables/live_grapher/data/', id, '.json')
    where datasetId in (
    select id from (
        select
        id
        from datasets
        where (
                -- must be used in at least one chart
                id in (
                    select distinct v.datasetId from chart_dimensions as cd
                    join variables as v on cd.variableId = v.id
                )
            ) and not isArchived
    ) as t
    );
    ```
    """
    engine = get_engine()

    datasets = _load_datasets(engine, dataset_ids, dt_start)
    if len(datasets) == 0:
        log.info("datasync.no_new_datasets")
        return

    log.info("datasync.loaded_datasets", n=len(datasets))
    client = connect_s3(
        s3_config=Config(
            retries={
                "max_attempts": 10,
                "mode": "adaptive",
            }
        )
    )

    for n, ds in enumerate(datasets):
        progress = f"{n+1}/{len(datasets)}"
        try:
            ds_s3 = DatasetSync.load_from_s3(client, ds.id)
            # datasets match, we don't have to sync to S3
            if not force and ds.matches(ds_s3):
                log.info("datasync.skip", dataset_id=ds.id, progress=progress)
                continue
        except client.exceptions.NoSuchKey:
            pass

        log.info("datasync.start", dataset_id=ds.id, progress=progress)

        variable_ids = _load_variable_ids(engine, ds.id)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            results = executor.map(
                lambda variable_id: _sync_variable_data_metadata(
                    variable_id=variable_id,
                    engine=engine,
                    dry_run=dry_run,
                    private=ds.isPrivate,
                ),
                variable_ids,
            )

            # raise errors from futures, otherwise they'd fail silently
            list(results)

        # add file to S3
        if not dry_run:
            ds.save_to_s3(client)

        log.info("datasync.end", dataset_id=ds.id)


def _sync_variable_data_metadata(engine: Engine, variable_id: int, dry_run: bool, private: bool) -> None:
    t = time.time()
    variable_df = variable_data_df_from_mysql(engine, variable_id)

    # if data_values is empty, try loading data from S3 to use it for dimensions
    # NOTE: if metadata changes, we still reupload even data to S3, this is quite inefficient, but
    #   this entire script is a temporary solution until everything is uploaded directly from ETL
    if variable_df.empty:
        variable_df = variable_data_df_from_s3(engine, _variable_dataPath(variable_id))

    var_data = variable_data(variable_df)
    var_metadata = variable_metadata(engine, variable_id, variable_df)

    if not dry_run:
        # upload data and metadata to S3
        _upload_gzip_dict(var_data, _variable_data_s3_key(variable_id), private)
        _upload_gzip_dict(var_metadata, _variable_metadata_s3_key(variable_id), private)

        # update dataPath and metadataPath of a variable
        with Session(engine) as session:
            variable = gm.Variable.load_variable(session, variable_id)
            variable.dataPath = _variable_dataPath(variable_id)
            variable.metadataPath = _variable_metadataPath(variable_id)
            session.add(variable)
            session.commit()

    log.info("datasync.upload", t=f"{time.time() - t:.2f}s", variable_id=variable_id)


def _upload_gzip_dict(d: Dict[str, Any], key: str, private: bool) -> None:
    body_gzip = gzip.compress(json.dumps(d, default=str).encode())  # type: ignore

    for attempt in Retrying(
        wait=wait_exponential(min=5, max=100),
        stop=stop_after_attempt(7),
        retry=retry_if_exception_type(EndpointConnectionError),
    ):
        with attempt:
            client = connect_s3()
            client.put_object(
                Bucket=S3_BUCKET,
                Body=body_gzip,
                Key=key,
                ContentEncoding="gzip",
                ContentType="application/json",
                ACL="private" if private else "public-read",
            )


@dataclass_json
@dataclass
class DatasetSync:
    id: int
    dataEditedAt: dt.datetime
    metadataEditedAt: dt.datetime
    isPrivate: bool
    sourceChecksum: str

    def to_dict(self) -> Dict[str, Any]:
        ...

    @classmethod
    def load_from_s3(cls, client, dataset_id):
        a = client.get_object(
            Bucket=S3_BUCKET,
            Key=_dataset_success_s3_key(dataset_id),
        )
        d = json.loads(a["Body"].read().decode())
        d["dataEditedAt"] = dt.datetime.utcfromtimestamp(d["dataEditedAt"] / 1000)
        d["metadataEditedAt"] = dt.datetime.utcfromtimestamp(d["metadataEditedAt"] / 1000)
        d.pop("name", None)
        return cls(**d)

    def save_to_s3(self, client):
        client.put_object(
            Bucket=S3_BUCKET,
            Body=pd.Series(self.to_dict()).to_json(),
            Key=_dataset_success_s3_key(self.id),
            ContentType="application/json",
            ACL="public-read",
        )

    def matches(self, ds: "DatasetSync") -> bool:
        # compare timestamps of the latest update (we don't have sourceChecksum for all datasets)
        # NOTE: we rebake both data and metadata even if only one has changed. This is a bit inefficient, but should
        # not matter much since dataset updates are rare.
        if self.dataEditedAt == ds.dataEditedAt and self.metadataEditedAt == ds.metadataEditedAt:
            # checksums should match if dataEditedAt match
            assert self.sourceChecksum is None or ds.sourceChecksum is None or self.sourceChecksum == ds.sourceChecksum
            return True
        else:
            return False


def _variable_data_s3_key(variable_id: int) -> str:
    return f"{S3_PREFIX}/data/{variable_id}.json"


def _variable_metadata_s3_key(variable_id: int) -> str:
    return f"{S3_PREFIX}/metadata/{variable_id}.json"


def _dataset_success_s3_key(dataset_id: int) -> str:
    return f"{S3_PREFIX}/success/_success_dataset_{dataset_id}"


def _variable_dataPath(variable_id: int) -> str:
    return os.path.join(S3_ENDPOINT, _variable_data_s3_key(variable_id))


def _variable_metadataPath(variable_id: int) -> str:
    return os.path.join(S3_ENDPOINT, _variable_metadata_s3_key(variable_id))


def _load_variable_ids(engine: Engine, dataset_id: int) -> List[int]:
    q = """
    select id from variables where datasetId = %(dataset_id)s
    """
    return pd.read_sql(q, engine, params={"dataset_id": dataset_id})["id"]


def _load_datasets(engine: Engine, dataset_ids: tuple[int], dt_start: Optional[dt.datetime]) -> List[DatasetSync]:
    if dataset_ids:
        where = "id in %(dataset_ids)s"
    elif dt_start:
        where = "dataEditedAt > %(dt_start)s or metadataEditedAt > %(dt_start)s"
    else:
        # load datasets with at least one chart
        where = """
        (
            -- must be used in at least one chart
            id in (
                select distinct v.datasetId from chart_dimensions as cd
                join variables as v on cd.variableId = v.id
            )
        )
        -- we would like to exclude archived datasets, but we cannot do it
        -- because some charts still use them (e.g. dataset 88)
        """

    q = f"""
    select
        id,
        dataEditedAt,
        metadataEditedAt,
        -- we save all variables as public for now to have them available in grapher
        false as isPrivate,
        sourceChecksum
    from datasets
    where {where}
    """
    df = pd.read_sql(
        q,
        engine,
        params={
            "dataset_ids": dataset_ids,
            "dt_start": dt_start,
        },
    )

    ds_dicts = df.to_dict(orient="records")

    for ds in ds_dicts:
        ds["dataEditedAt"] = ds["dataEditedAt"].to_pydatetime()
        ds["metadataEditedAt"] = ds["metadataEditedAt"].to_pydatetime()

    return [DatasetSync(**d) for d in ds_dicts]

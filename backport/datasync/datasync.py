import concurrent.futures
import datetime as dt
import gzip
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import rich_click as click
import structlog
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError
from dataclasses_json import dataclass_json
from owid.catalog import s3_utils
from sqlalchemy.engine import Engine
from sqlmodel import Session
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from backport.datasync.data_metadata import (
    variable_data,
    variable_data_df_from_mysql,
    variable_data_df_from_s3,
    variable_metadata,
)
from etl import config
from etl import grapher_model as gm
from etl.db import get_engine
from etl.publish import connect_s3, connect_s3_cached

log = structlog.get_logger()

config.enable_bugsnag()


@click.command(help=__doc__)
@click.option("--dataset-ids", "-d", type=int, multiple=True)
@click.option("--variable-ids", "-v", type=int, multiple=True)
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
    variable_ids: tuple[int],
    dt_start: dt.datetime,
    dry_run: bool,
    force: bool,
    workers: int,
) -> None:
    """
    Sync data_values and metadata from MySQL to s3://owid-catalog/baked-variables/[db_name]/data/[variable_id].json files
    with structure that can be consumed by the grapher.

    Once variable is replicated, we point its dataPath column in MySQL to the file.

    This is intended to run as continuous service at the moment, but in the long run we should be creating those JSON files
    when running etl --grapher.

    Usage:
        etl-datasync ...

        # sync dataset
        ENV=.env.staging backport-datasync -d 2405

        # sync all datasets since some date
        ENV=.env.staging backport-datasync --dt-start "2023-01-01 00:00:00" --workers 10

        # real-time sync in a forever loop
        last_dt="2023-01-01 00:00:00"
        while true; do
            new_dt=$(date +"%Y-%m-%d %H:%M:%S")
            ENV=.env.staging backport-datasync --workers 10 --dt-start "$last_dt"
            last_dt=$new_dt
            sleep 5
        done;
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

        ds_variable_ids = variable_ids or _load_variable_ids(engine, ds.id)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            results = executor.map(
                lambda variable_id: _sync_variable_data_metadata(
                    variable_id=variable_id,
                    engine=engine,
                    dry_run=dry_run,
                    private=ds.isPrivate,
                ),
                ds_variable_ids,
            )

            # raise errors from futures, otherwise they'd fail silently
            list(results)

        # add file to S3 if not dry run or not syncing specific variables
        if not dry_run and not variable_ids:
            ds.save_to_s3(client)

        log.info("datasync.end", dataset_id=ds.id)


def _sync_variable_data_metadata(engine: Engine, variable_id: int, dry_run: bool, private: bool) -> None:
    t = time.time()
    variable_df = variable_data_df_from_mysql(engine, variable_id)

    with Session(engine) as session:
        variable = gm.Variable.load_variable(session, variable_id)

        # if data_values is empty, try loading data from S3 to use it for dimensions
        # NOTE: if metadata changes, we still reupload even data to S3, this is quite inefficient, but
        #   this entire script is a temporary solution until everything is uploaded directly from ETL
        if variable_df.empty:
            assert variable.dataPath
            variable_df = variable_data_df_from_s3(engine, [variable.dataPath])

        var_data = variable_data(variable_df)
        var_metadata = variable_metadata(engine, variable_id, variable_df)

        if not dry_run:
            # upload data and metadata to S3
            data_path = upload_gzip_dict(var_data, variable.s3_data_path(), private)
            metadata_path = upload_gzip_dict(var_metadata, variable.s3_metadata_path(), private)

            # update dataPath and metadataPath of a variable if different
            if variable.dataPath != data_path or variable.metadataPath != metadata_path:
                variable.dataPath = data_path
                variable.metadataPath = metadata_path
                session.add(variable)
                session.commit()

    log.info("datasync.upload", t=f"{time.time() - t:.2f}s", variable_id=variable_id)


def upload_gzip_dict(d: Dict[str, Any], s3_path: str, private: bool = False) -> str:
    """Upload compressed dictionary to S3 and return its URL."""
    body_gzip = gzip.compress(json.dumps(d, default=str).encode())  # type: ignore

    bucket, key = s3_utils.s3_bucket_key(s3_path)

    client = connect_s3_cached()

    for attempt in Retrying(
        wait=wait_exponential(min=5, max=100),
        stop=stop_after_attempt(7),
        retry=retry_if_exception_type(EndpointConnectionError),
    ):
        with attempt:
            client.put_object(
                Bucket=bucket,
                Body=body_gzip,
                Key=key,
                ContentEncoding="gzip",
                ContentType="application/json",
                ACL="private" if private else "public-read",
            )

    # bucket owid-catalog is behind Cloudflare
    if bucket == "owid-catalog":
        return f"https://catalog.ourworldindata.org/{key}"
    else:
        return f"https://{bucket}.nyc3.digitaloceanspaces.com/{key}"


@dataclass_json
@dataclass
class DatasetSync:
    id: int
    dataEditedAt: dt.datetime
    metadataEditedAt: dt.datetime
    variablesEditedAt: dt.datetime
    isPrivate: bool
    sourceChecksum: str

    def to_dict(self) -> Dict[str, Any]:
        ...

    @classmethod
    def load_from_s3(cls, client, dataset_id):
        bucket, key = s3_utils.s3_bucket_key(_dataset_success_s3_path(dataset_id))
        a = client.get_object(
            Bucket=bucket,
            Key=key,
        )
        d = json.loads(a["Body"].read().decode())
        d["dataEditedAt"] = dt.datetime.utcfromtimestamp(d["dataEditedAt"] / 1000)
        d["metadataEditedAt"] = dt.datetime.utcfromtimestamp(d["metadataEditedAt"] / 1000)
        if "variablesEditedAt" in d:
            d["variablesEditedAt"] = dt.datetime.utcfromtimestamp(d["variablesEditedAt"] / 1000)
        else:
            d["variablesEditedAt"] = d["metadataEditedAt"]
        d.pop("name", None)
        return cls(**d)

    def save_to_s3(self, client):
        bucket, key = s3_utils.s3_bucket_key(_dataset_success_s3_path(self.id))
        client.put_object(
            Bucket=bucket,
            Body=pd.Series(self.to_dict()).to_json(),
            Key=key,
            ContentType="application/json",
            ACL="public-read",
        )

    def matches(self, ds: "DatasetSync") -> bool:
        # compare timestamps of the latest update (we don't have sourceChecksum for all datasets)
        # NOTE: we rebake both data and metadata even if only one has changed. This is a bit inefficient, but should
        # not matter much since dataset updates are rare.
        if (
            self.dataEditedAt == ds.dataEditedAt
            and self.metadataEditedAt == ds.metadataEditedAt
            and self.variablesEditedAt == ds.variablesEditedAt
        ):
            # checksums should match if dataEditedAt match
            assert self.sourceChecksum is None or ds.sourceChecksum is None or self.sourceChecksum == ds.sourceChecksum
            return True
        else:
            return False


def _dataset_success_s3_path(dataset_id: int) -> str:
    return f"{config.BAKED_VARIABLES_PATH}/success/_success_dataset_{dataset_id}"


def _load_variable_ids(engine: Engine, dataset_id: int) -> List[int]:
    q = """
    select id from variables where datasetId = %(dataset_id)s
    """
    return pd.read_sql(q, engine, params={"dataset_id": dataset_id})["id"]


def _load_datasets(engine: Engine, dataset_ids: tuple[int], dt_start: Optional[dt.datetime]) -> List[DatasetSync]:
    if dataset_ids:
        where = "d.id in %(dataset_ids)s"
        having = "1 = 1"
    elif dt_start:
        # datasets with shortName come from ETL and are already synced
        # we don't use sourceChecksum because that one is set only after the dataset is fully upserted
        where = "d.shortName is null and v.shortName is null"
        having = "dataEditedAt > %(dt_start)s or metadataEditedAt > %(dt_start)s or variablesEditedAt > %(dt_start)s"

    else:
        raise ValueError("Either dataset_ids or dt_start must be specified")

    q = f"""
    select
        MAX(d.id) as id,
        MAX(d.dataEditedAt) as dataEditedAt,
        MAX(d.metadataEditedAt) as metadataEditedAt,
        -- we save all variables as public for now to have them available in grapher
        false as isPrivate,
        MAX(d.sourceChecksum) as sourceChecksum,
        -- latest updated variable
        MAX(v.updatedAt) as variablesEditedAt
    from datasets as d
    join variables as v on v.datasetId = d.id
    where {where}
    group by d.id
    having {having}
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
        ds["variablesEditedAt"] = ds["variablesEditedAt"].to_pydatetime()

    return [DatasetSync(**d) for d in ds_dicts]


if __name__ == "__main__":
    cli()

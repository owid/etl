import os
import tempfile
import time
from typing import Optional

import click
import pandas as pd
import structlog
from owid.catalog.utils import validate_underscore
from owid.walden import Catalog as WaldenCatalog
from owid.walden.catalog import Dataset as WaldenDataset
from owid.walden.ingest import add_to_catalog
from sqlalchemy.engine import Engine

from etl.db import get_engine
from etl.files import checksum_str
from etl.grapher_model import (GrapherConfig, GrapherDatasetModel,
                               GrapherSourceModel, GrapherVariableModel)

WALDEN_NAMESPACE = os.environ.get("WALDEN_NAMESPACE", "backport")

log = structlog.get_logger()


def _walden_values_metadata(
    ds: GrapherDatasetModel, short_name: str, origin_md5: str
) -> WaldenDataset:
    """Create walden dataset for grapher dataset values.
    These datasets are not meant for direct consumption from the catalog, but rather
    for postprocessing in etl.
    :param short_name: short name of the dataset in catalog
    :param origin_md5: MD5 hash of data values in grapher used to decide whether to recompute
        or not in the next run
    """
    return WaldenDataset(
        namespace=WALDEN_NAMESPACE,
        short_name=f"{short_name}_values",
        name=ds.name,
        description=ds.description,
        source_name="Our World in Data",
        url=f"https://owid.cloud/admin/datasets/{ds.id}",
        publication_date="latest",
        file_extension="feather",
        origin_md5=origin_md5,
    )


def _walden_config_metadata(
    ds: GrapherDatasetModel, short_name: str, origin_md5: str
) -> WaldenDataset:
    """Create walden dataset for grapher dataset variables and metadata."""
    config = _walden_values_metadata(ds, short_name, origin_md5)
    config.short_name = short_name + "_config"
    config.name = f"Grapher metadata for {short_name}"
    config.file_extension = "json"
    return config


def _load_values(engine: Engine, variable_ids: list[int]) -> pd.DataFrame:
    """Get data values of a variable."""
    # NOTE: loading entity_name and variable_name is perhaps unnecessary, consider removing it
    # to speed up loading
    q = f"""
    select
        d.entityId as entity_id,
        d.variableId as variable_id,
        -- it would be more efficient to load entity name and variable name separately and
        -- then join it before uploading to walden
        e.name as entity_name,
        v.name as variable_name,
        d.year,
        d.value as value
    from data_values as d
    join entities as e on e.id = d.entityId
    join variables as v on v.id = d.variableId
    where d.variableId in %(variable_ids)s
    """
    df: pd.DataFrame = pd.read_sql(q, engine, params={"variable_ids": variable_ids})

    # try converting values to float if possible, this can make the data 50% smaller
    # if successful
    df["value"] = df["value"].astype(float, errors="ignore")

    # use categoricals for smaller files (significant reduction even with compression)
    df = df.astype(
        {
            "variable_id": "category",
            "variable_name": "category",
            "entity_id": "category",
            "entity_name": "category",
        }
    )
    return df


def _load_config(
    ds: GrapherDatasetModel,
    vars: list[GrapherVariableModel],
    sources: list[GrapherSourceModel],
) -> GrapherConfig:
    """Get the configuration of a variable."""
    return GrapherConfig(
        dataset=ds,
        variables=vars,
        sources=sources,
    )


def _upload_config_to_walden(
    config: GrapherConfig, meta: WaldenDataset, dry_run: bool
) -> None:
    with tempfile.NamedTemporaryFile(mode="w") as f:
        f.write(config.json())
        f.flush()

        if not dry_run:
            add_to_catalog(meta, f.name, upload=True)


def _upload_values_to_walden(
    df: pd.DataFrame,
    meta: WaldenDataset,
    dry_run: bool,
) -> None:
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        df.to_feather(f.name, compression="lz4")
        if not dry_run:
            add_to_catalog(meta, f.name, upload=True)


def _content_hash_values(engine: Engine, variable_ids: list[int]) -> str:
    """Get content hash of values for given variables. This version is quite slow,
    processing WBI dataset (7.7M data values) takes 33s.

    An alternative to checking content sum of all data values would be creating a
    changelog table for variables that would get updated by MySQL trigger whenever
    a data value is updated. (or adding a column to existing variables table)

    An example (taken from https://stackoverflow.com/questions/4753878/how-to-program-a-mysql-trigger-to-insert-row-into-another-table)
    ```
    delimiter #
    create trigger variable_changelog_trig after insert on data_values
    for each row
    begin
        insert into variable_changelog (variableId, updatedAt) values (variableId, NOW())
        on duplicate key update
            updatedAt = VALUES(updatedAt)
    end#
    ```

    However, this might be quite inefficient for large data values. Better solution would be to
    update variable `updatedAt` column on app level whenever its data value is updated.
    """
    q = """
    SELECT MD5(
        GROUP_CONCAT(
            CONCAT_WS('#',value,year,entityId,variableId) SEPARATOR '##'
        )
    ) FROM data_values
    where variableId in %(variable_ids)s
    order by year, entityId, variableId
    """
    log.info("backport.content_hash_values.start", variable_ids=variable_ids)
    t = time.time()
    content_hash = engine.execute(q, variable_ids=variable_ids).first()[0]
    assert content_hash is not None
    log.info("backport.content_hash_values.end", hash=content_hash, t=time.time() - t)
    return content_hash


def _checksums_match(short_name: str, md5_config: str, md5_values: str) -> bool:
    try:
        walden_ds_config = WaldenCatalog().find_one(short_name=f"{short_name}_config")
        walden_ds_values = WaldenCatalog().find_one(short_name=f"{short_name}_values")
    except KeyError:
        # datasets not found in catalog
        return False

    return (
        walden_ds_config.origin_md5 == md5_config
        and walden_ds_values.origin_md5 == md5_values
    )


def _create_short_name(
    short_name: Optional[str], dataset_id: int, variable_id: int
) -> str:
    """Create sensible short name for dataset."""
    if short_name:
        validate_underscore(short_name, "short-name")
        # prepend dataset id to short name
        return f"{dataset_id}_{short_name}"
    else:
        if variable_id:
            return f"{dataset_id}_{variable_id}"
        else:
            return str(dataset_id)


@click.command()
@click.option("--dataset-id", type=int)
@click.option("--variable-id", type=int)
@click.option(
    "--short-name", type=str, help="Short name of a dataset, must be under_score"
)
@click.option(
    "--force/--no-force",
    default=False,
    type=bool,
    help="Force overwrite even if checksums match",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
    help="Do not add dataset to a catalog on dry-run",
)
def backport(
    dataset_id: int,
    variable_id: int,
    short_name: str,
    force: bool,
    dry_run: bool,
) -> None:
    engine = get_engine()

    # get data from database
    log.info("backport.loading_dataset", dataset_id=dataset_id)
    ds = GrapherDatasetModel.load_dataset(engine, dataset_id)
    log.info("backport.loading_variable", variable_id=variable_id or "all")
    if variable_id:
        vars = [GrapherVariableModel.load_variable(engine, variable_id)]
    else:
        # load all variables from a dataset
        vars = GrapherDatasetModel.load_variables_for_dataset(engine, dataset_id)
    variable_ids = [v.id for v in vars]

    # get sources for dataset and all variables
    sources = GrapherSourceModel.load_sources(
        engine, dataset_id=ds.id, variable_ids=variable_ids
    )

    short_name = _create_short_name(short_name, dataset_id, variable_id)

    config = _load_config(ds, vars, sources)

    # get checksums of config and values
    md5_config = checksum_str(config.json(sort_keys=True, indent=0))
    md5_values = _content_hash_values(engine, variable_ids)

    # if checksums of data and config are identical, skip upload
    if not force:
        if _checksums_match(short_name, md5_config, md5_values):
            log.info("backport.skip", short_name=short_name, reason="checksums match")
            return

    # upload config to walden
    log.info("backport.upload_config")
    _upload_config_to_walden(
        config, _walden_config_metadata(ds, short_name, md5_config), dry_run
    )

    # upload values to walden
    log.info("backport.loading_values", variables=variable_ids)
    df = _load_values(engine, variable_ids)
    log.info("backport.upload_values", size=len(df))
    _upload_values_to_walden(
        df, _walden_values_metadata(ds, short_name, md5_values), dry_run
    )

    log.info("backport.finished")


if __name__ == "__main__":
    # Example (run against staging DB):
    #   backport --dataset-id 5426 --variable-id 244087 --name political_regimes --dry-run --force
    #   or entire dataset
    #   backport --dataset-id 5426 --short-name political_regimes --force
    backport()

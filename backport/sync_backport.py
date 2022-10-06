from typing import List, cast

import pandas as pd
import rich_click as click
import structlog
from owid.catalog import LocalCatalog
from sqlmodel import Session, delete

from etl import config
from etl import grapher_model as gm
from etl.db import get_engine
from etl.steps import paths

config.enable_bugsnag()
log = structlog.get_logger()
engine = get_engine()


@click.command()
@click.option("--dataset-ids", "-d", type=int, multiple=True)
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--force", is_flag=True, default=False)
@click.option(
    "--prune-data-values",
    is_flag=True,
    default=False,
    help="Remove data from data_values table. This can take a long time.",
)
def cli(dataset_ids: tuple[int], dry_run: bool, force: bool, prune_data_values: bool) -> None:
    """Sync backported datasets back to MySQL.

    We iterate through all backported datasets and update their `variables.catalogPath` field
    in MySQL. Variables having non-null catalogPath load their values from the catalog instead
    of data_values table.

    If new dataset is uploaded from admin, we should set `catalogPath` back to null and fetch
    values from `data_values` table. In case someone renames a dataset we have remove catalogPath
    too (honestly it makes sense to do it on any kind of update).

    Note that private datasets are not backported, so they remain in data_values (like COVID data).
    """
    lc = LocalCatalog(paths.DATA_DIR, channels=["backport"])
    frame = lc.frame
    frame["dataset_id"] = frame.dataset.str.split("_").str.get(1).astype(int)
    if dataset_ids:
        frame = frame[frame.dataset_id.isin(dataset_ids)]

    all_variables = _variables_to_update(list(frame.dataset_id.unique()), force)

    log.info("sync_backport.start", variables_to_update=all_variables.shape[0])

    for dataset_id, vars in all_variables.groupby("datasetId"):
        # one backported dataset can have multiple tables if it is large
        # iterate over all of them
        dataset_tables = frame[frame.dataset_id == dataset_id]

        for _, catalog_table in dataset_tables.iterrows():
            catalog_path = catalog_table.path
            table = catalog_table.load()

            for col in table.columns:
                col_meta = table[col].metadata.additional_info["grapher_meta"]
                var_id: int = col_meta["id"]

                # if variable is not in vars it means it doesn't need to be updated
                if var_id not in vars.index:
                    continue

                # is backported variable the same one as in MySQL?
                if col_meta["updatedAt"] == str(vars.loc[var_id, "updatedAt"]):
                    # update catalogPath and shortName in MySQL
                    if not dry_run:
                        # does it need updating?
                        if vars.loc[var_id].catalogPath != catalog_path or vars.loc[var_id].shortName != col:
                            _update_variable_in_db(var_id, col, catalog_path, prune_data_values)
                            log.info(
                                "sync_backport.variable.update",
                                var_id=var_id,
                                short_name=col,
                                catalog_path=catalog_path,
                            )

                        if prune_data_values:
                            _prune_data_values(var_id)

                else:
                    # this should be very rare (unless you run it locally and your DB is out of sync)
                    log.info("sync_backport.variable.out_of_sync", var_id=var_id)


def _update_variable_in_db(var_id: int, short_name: str, catalog_path: str, prune_data_values: bool) -> None:
    with Session(engine) as session:
        db_var = gm.Variable.load_variable(session, var_id)
        db_var.catalogPath = catalog_path
        db_var.shortName = short_name

        if prune_data_values:
            session.execute(delete(gm.DataValues).where(gm.DataValues.variableId == var_id))

        session.commit()


def _prune_data_values(var_id: int) -> None:
    with Session(engine) as session:
        session.execute(delete(gm.DataValues).where(gm.DataValues.variableId == var_id))
        session.commit()


def _variables_to_update(dataset_ids: List[int], force: bool) -> pd.DataFrame:
    """Get all variables without catalogPath, without updatedAt or those
    that have been updated recently (in case either dataset or their name
    have been updated).

    :param force: update all variables irrespective of catalogPath and updatedAt
    """
    q = f"""
    select
        id, datasetId, updatedAt, catalogPath, shortName
    from variables
    where datasetId in %(dataset_ids)s
        {'' if force else 'and (updatedAt >= DATE_SUB(NOW(), INTERVAL 1 DAY) or catalogPath is null or shortName is null)'}
    """
    all_variables = pd.read_sql(
        q,
        engine,
        params={
            "dataset_ids": dataset_ids,
        },
    )
    return cast(pd.DataFrame, all_variables.set_index("id"))


if __name__ == "__main__":
    cli()

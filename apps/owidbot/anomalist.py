from structlog import get_logger

from apps.anomalist.anomalist_api import anomaly_detection
from apps.wizard.app_pages.anomalist.utils import load_variable_mapping
from etl import grapher_model as gm
from etl.config import OWIDEnv
from etl.db import Engine, read_sql

from .chart_diff import production_or_master_engine

log = get_logger()


def run(branch: str) -> None:
    """Compute all anomalist for new and updated datasets."""
    # Get engines for branch and production
    source_engine = OWIDEnv.from_staging(branch).get_engine()
    target_engine = production_or_master_engine()

    # Create table with anomalist if it doesn't exist
    gm.Anomaly.create_table(source_engine, if_exists="skip")

    # Load new dataset ids
    datasets_new_ids = _load_datasets_new_ids(source_engine, target_engine)

    if not datasets_new_ids:
        log.info("No new datasets found.")
        return

    log.info(f"New datasets: {datasets_new_ids}")

    # Load all their variables
    q = """SELECT id FROM variables WHERE datasetId IN %(dataset_ids)s"""
    variable_ids = list(read_sql(q, source_engine, params={"dataset_ids": datasets_new_ids})["id"])

    # Load variable mapping
    variable_mapping_dict = load_variable_mapping(datasets_new_ids)

    # Run anomalist
    anomaly_detection(
        variable_mapping=variable_mapping_dict,
        variable_ids=variable_ids,
    )


def _load_datasets_new_ids(source_engine: Engine, target_engine: Engine) -> list[int]:
    # Get new datasets
    # TODO: replace by real catalogPath when we have it in MySQL
    q = """SELECT
        id,
        CONCAT(namespace, "/", version, "/", shortName) as catalogPath
    FROM datasets
    """
    source_datasets = read_sql(q, source_engine)
    target_datasets = read_sql(q, target_engine)

    return list(
        source_datasets[
            source_datasets.catalogPath.isin(set(source_datasets["catalogPath"]) - set(target_datasets["catalogPath"]))
        ]["id"]
    )

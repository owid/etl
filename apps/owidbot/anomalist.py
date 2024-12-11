import time

from sqlalchemy.orm import Session
from structlog import get_logger

from apps.anomalist.anomalist_api import anomaly_detection
from apps.anomalist.cli import load_datasets_new_ids
from apps.wizard.app_pages.anomalist.utils import load_variable_mapping
from apps.wizard.utils.io import get_new_grapher_datasets_and_their_previous_versions
from etl import grapher_model as gm
from etl.config import OWIDEnv
from etl.db import read_sql

log = get_logger()


def run(branch: str) -> None:
    """Compute all anomalist for new and updated datasets."""
    # Get engines for branch and production
    source_engine = OWIDEnv.from_staging(branch).get_engine()

    # Create table with anomalist if it doesn't exist
    gm.Anomaly.create_table(source_engine, if_exists="skip")

    # Load new dataset ids
    datasets_new_ids = load_datasets_new_ids(source_engine)

    # Append datasets with changed local files. This is done to be compatible with the Anomalist streamlit app.
    with Session(source_engine) as session:
        datasets_new_ids = list(
            set(datasets_new_ids) | set(get_new_grapher_datasets_and_their_previous_versions(session=session))
        )

    if not datasets_new_ids:
        log.info("No new datasets found.")
        return

    log.info(f"New datasets: {datasets_new_ids}")

    # Load all their variables
    q = """SELECT id FROM variables WHERE datasetId IN %(dataset_ids)s"""
    variable_ids = list(read_sql(q, source_engine, params={"dataset_ids": datasets_new_ids})["id"])

    # Load variable mapping
    variable_mapping_dict = load_variable_mapping(datasets_new_ids)

    log.info("owidbot.anomalist.start", n_variables=len(variable_ids))
    t = time.time()

    # Run anomalist
    anomaly_detection(
        variable_mapping=variable_mapping_dict,
        variable_ids=variable_ids,
    )

    log.info("owidbot.anomalist.end", n_variables=len(variable_ids), t=time.time() - t)

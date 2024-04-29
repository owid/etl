"""Utils for chart revision tool."""
from typing import Any, Dict, Tuple

import pandas as pd
import streamlit as st
from MySQLdb import OperationalError
from structlog import get_logger

from etl.chart_revision.v3.schema import get_schema_chart_config
from etl.db import config, get_all_datasets, get_connection, get_variables_in_dataset

# Logger
log = get_logger()


@st.cache_data(show_spinner=False)
def get_datasets() -> pd.DataFrame:
    """Load datasets."""
    with st.spinner("Retrieving datasets..."):
        try:
            datasets = get_all_datasets(archived=False)
        except OperationalError as e:
            raise OperationalError(
                f"Could not retrieve datasets. Try reloading the page. If the error persists, please report an issue. Error: {e}"
            )
        else:
            return datasets.sort_values("name")


@st.cache_data(show_spinner=False)
def get_schema() -> Dict[str, Any]:
    """Load datasets."""
    with st.spinner("Retrieving schema..."):
        try:
            schema = get_schema_chart_config()
        except OperationalError as e:
            raise OperationalError(
                f"Could not retrieve the schema. Try reloading the page. If the error persists, please report an issue. Error: {e.__traceback__}"
            )
        else:
            return schema


@st.cache_data
def get_indicators_from_datasets(
    dataset_id_1: int, dataset_id_2: int, show_new_not_in_old: int = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get indicators from two datasets."""
    with get_connection() as db_conn:
        # Get indicators from old dataset that have been used in at least one chart.
        old_indictors = get_variables_in_dataset(db_conn=db_conn, dataset_id=dataset_id_1, only_used_in_charts=True)
        # Get all indicators from new dataset.
        new_indictors = get_variables_in_dataset(db_conn=db_conn, dataset_id=dataset_id_2, only_used_in_charts=False)
        if show_new_not_in_old:
            # Unsure why this was done, but it seems to be wrong.
            # Remove indicators in the new dataset that are not in the old dataset.
            # This can happen if we are matching a dataset to itself in case of renaming variables.
            new_indictors = new_indictors[~new_indictors["id"].isin(old_indictors["id"])]
    return old_indictors, new_indictors


def _check_env() -> bool:
    """Check if environment indicators are set correctly."""
    ok = True
    for env_name in ("GRAPHER_USER_ID", "DB_USER", "DB_NAME", "DB_HOST"):
        if getattr(config, env_name) is None:
            ok = False
            st.warning(st.markdown(f"Environment variable `{env_name}` not found, do you have it in your `.env` file?"))

    if ok:
        st.success("`.env` configured correctly")
    return ok


def _show_environment() -> None:
    """Show environment indicators (streamlit)."""
    # show indicators (from .env)
    st.info(
        f"""
    * **GRAPHER_USER_ID**: `{config.GRAPHER_USER_ID}`
    * **DB_USER**: `{config.DB_USER}`
    * **DB_NAME**: `{config.DB_NAME}`
    * **DB_HOST**: `{config.DB_HOST}`
    """
    )


@st.cache_resource
def _check_env_and_environment() -> None:
    """Check if environment indicators are set correctly."""
    ok = _check_env()
    if ok:
        # check that you can connect to DB
        try:
            with st.spinner():
                _ = get_connection()
        except OperationalError as e:
            st.error(
                "We could not connect to the database. If connecting to a remote database, remember to"
                f" ssh-tunel into it using the appropriate ports and then try again.\n\nError:\n{e}"
            )
            ok = False
        except Exception as e:
            raise e
        else:
            msg = "Connection to the Grapher database was successfull!"
            st.success(msg)
            st.subheader("Environment")
            _show_environment()

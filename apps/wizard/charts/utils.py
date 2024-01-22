"""Utils for chart revision tool."""
from typing import Any, Dict, Literal, Optional, Tuple

import pandas as pd
import streamlit as st
from MySQLdb import OperationalError
from structlog import get_logger
from typing_extensions import Self

from etl.chart_revision.v2.schema import get_schema_chart_config
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
    with st.spinner("Retrieving chart config schema..."):
        try:
            schema = get_schema_chart_config()
        except OperationalError as e:
            raise OperationalError(
                f"Could not retrieve the schema. Try reloading the page. If the error persists, please report an issue. Error: {e.__traceback__}"
            )
        else:
            return schema


def get_variables_from_datasets(
    dataset_id_1: int, dataset_id_2: int, show_new_not_in_old: int = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get variables from two datasets."""
    with get_connection() as db_conn:
        # Get variables from old dataset that have been used in at least one chart.
        old_variables = get_variables_in_dataset(db_conn=db_conn, dataset_id=dataset_id_1, only_used_in_charts=True)
        # Get all variables from new dataset.
        new_variables = get_variables_in_dataset(db_conn=db_conn, dataset_id=dataset_id_2, only_used_in_charts=False)
        if show_new_not_in_old:
            # Unsure why this was done, but it seems to be wrong.
            # Remove variables in the new dataset that are not in the old dataset.
            # This can happen if we are matching a dataset to itself in case of renaming variables.
            new_variables = new_variables[~new_variables["id"].isin(old_variables["id"])]
    return old_variables, new_variables


def _check_env() -> bool:
    """Check if environment variables are set correctly."""
    ok = True
    for env_name in ("GRAPHER_USER_ID", "DB_USER", "DB_NAME", "DB_HOST"):
        if getattr(config, env_name) is None:
            ok = False
            st.warning(st.markdown(f"Environment variable `{env_name}` not found, do you have it in your `.env` file?"))

    if ok:
        st.success("`.env` configured correctly")
    return ok


def _show_environment() -> None:
    """Show environment variables (streamlit)."""
    # show variables (from .env)
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
    """Check if environment variables are set correctly."""
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


OWIDEnvType = Literal["live", "staging", "local", "remote-staging", "unknown"]


class OWIDEnv:
    """OWID environment."""

    env_type_id: OWIDEnvType

    def __init__(
        self: Self,
        env_type_id: Optional[OWIDEnvType] = None,
    ) -> None:
        if env_type_id is None:
            self.env_type_id = self.detect_env_type()
        else:
            self.env_type_id = env_type_id

    def detect_env_type(self: Self) -> OWIDEnvType:
        """Detect environment type."""
        # live
        if config.DB_NAME == "live_grapher":
            return "live"
        # staging
        elif config.DB_NAME == "staging_grapher" and config.DB_USER == "staging_grapher":
            return "staging"
        # local
        elif config.DB_NAME == "grapher" and config.DB_USER == "grapher":
            return "local"
        # other
        elif config.DB_NAME == "owid" and config.DB_USER == "owid":
            return "remote-staging"
        return "unknown"

    @property
    def admin_url(
        self: Self,
    ) -> str | None:
        """Get admin url."""
        if self.env_type_id == "live":
            return "https://owid.cloud/admin"
        elif self.env_type_id == "staging":
            return "https://staging.owid.cloud/admin"
        elif self.env_type_id == "local":
            return "http://localhost:3030/admin"
        elif self.env_type_id == "remote-staging":
            return f"http://{config.DB_HOST}/admin"
        return None

    @property
    def chart_approval_tool_url(self: Self) -> str:
        """Get chart approval tool url."""
        return f"{self.admin_url}/suggested-chart-revisions/review"

    def dataset_admin_url(self: Self, dataset_id: str | int) -> str:
        """Get dataset admin url."""
        return f"{self.admin_url}/datasets/{dataset_id}/"

    def variable_admin_url(self: Self, variable_id: str | int) -> str:
        """Get variable admin url."""
        return f"{self.admin_url}/variables/{variable_id}/"

    def chart_admin_url(self: Self, chart_id: str | int) -> str:
        """Get chart admin url."""
        return f"{self.admin_url}/charts/{chart_id}/edit"

import json
import logging
import subprocess
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import structlog
from owid.catalog import find
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from apps.utils.map_datasets import get_grapher_changes
from etl.config import OWID_ENV, OWIDEnv
from etl.db import get_engine
from etl.git_helpers import get_changed_files
from etl.grapher import io as gio
from etl.grapher.model import Anomaly, Variable
from etl.version_tracker import VersionTracker

log = structlog.get_logger()

# silence WARNING streamlit.runtime.caching.cache_data_api: No runtime found, using MemoryCacheStorageManager
logging.getLogger("streamlit.runtime.caching.cache_data_api").setLevel(logging.ERROR)


@st.cache_data
def load_entity_ids(entity_ids: Optional[List[int]] = None):
    return gio.load_entity_mapping(entity_ids)


@st.cache_data(show_spinner=False)
def execute_bash_command(cmd):
    """Execute a command and get its output."""
    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return e.stderr


@st.cache_data
def load_variables_display_in_dataset(
    dataset_uri: Optional[List[str]] = None,
    dataset_id: Optional[List[int]] = None,
    only_slug: Optional[bool] = False,
    _owid_env: OWIDEnv = OWID_ENV,
) -> Dict[int, str]:
    """Load Variable objects that belong to a dataset with URI `dataset_uri`."""
    indicators = gio.load_variables_in_dataset(
        dataset_uri=dataset_uri,
        dataset_id=dataset_id,
        owid_env=_owid_env,
    )

    def _display_slug(o) -> str:
        p = o.catalogPath
        if only_slug:
            return p.rsplit("/", 1)[-1] if isinstance(p, str) else ""
        return p

    indicators_display = {i.id: _display_slug(i) for i in indicators}

    return indicators_display


@st.cache_data
def get_variable_uris(indicators: List[Variable], only_slug: Optional[bool] = False) -> List[str]:
    options = [o.catalogPath for o in indicators]
    if only_slug:
        options = [o.rsplit("/", 1)[-1] if isinstance(o, str) else "" for o in options]
    return options  # type: ignore


@st.cache_data
def load_dataset_uris_new_in_server() -> List[str]:
    """Load URIs of datasets that are new in staging server."""
    return gio.load_dataset_uris()


@st.cache_data
def load_dataset_uris() -> List[str]:
    return gio.load_dataset_uris()


@st.cache_data
def load_variables_in_dataset(
    dataset_uri: Optional[List[str]] = None,
    dataset_id: Optional[List[int]] = None,
    _owid_env: OWIDEnv = OWID_ENV,
) -> List[Variable]:
    """Load Variable objects that belong to a dataset with URI `dataset_uri`."""
    return gio.load_variables_in_dataset(
        dataset_uri=dataset_uri,
        dataset_id=dataset_id,
        owid_env=_owid_env,
    )


@st.cache_data
def load_variable_metadata(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    _owid_env: OWIDEnv = OWID_ENV,
) -> Dict[str, Any]:
    return gio.load_variable_metadata(
        catalog_path=catalog_path,
        variable_id=variable_id,
        variable=variable,
        owid_env=_owid_env,
    )


@st.cache_data
def load_variable_data(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    _owid_env: OWIDEnv = OWID_ENV,
) -> pd.DataFrame:
    return gio.load_variable_data(
        catalog_path=catalog_path,
        variable_id=variable_id,
        variable=variable,
        owid_env=_owid_env,
    )


@st.cache_data
def load_anomalies_in_dataset(
    dataset_ids: List[int],
    _owid_env: OWIDEnv = OWID_ENV,
) -> List[Anomaly]:
    """Load Anomaly objects that belong to a dataset with URI `dataset_uri`."""
    with Session(_owid_env.engine) as session:
        return Anomaly.load_anomalies(session, dataset_ids)


@st.cache_data(show_spinner=False)
def get_datasets_from_version_tracker() -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Get dataset info from version tracker (ETL)."""
    # Get steps_df
    vt = VersionTracker()
    assert vt.connect_to_db, "Can't connect to database! You need to be connected to run this tool."
    steps_df = vt.steps_df

    # Get file changes -> Infer dataset migrations
    files_changed = get_changed_files()
    grapher_changes = get_grapher_changes(files_changed, steps_df)

    # Only keep grapher steps
    steps_df_grapher = steps_df.loc[
        steps_df["channel"] == "grapher", ["namespace", "identifier", "step", "db_dataset_name", "db_dataset_id"]
    ]
    # Remove unneeded text from 'step' (e.g. '*/grapher/'), no need for fuzzymatch!
    steps_df_grapher["step_reduced"] = steps_df_grapher["step"].str.split("grapher/").str[-1]

    # Keep only those that are in DB (we need them to be in DB, otherwise indicator upgrade won't work since charts wouldn't be able to reference to non-db-existing indicator IDs)
    steps_df_grapher = steps_df_grapher.dropna(subset="db_dataset_id")
    assert steps_df_grapher.isna().sum().sum() == 0
    # Column rename
    steps_df_grapher = steps_df_grapher.rename(
        columns={
            "db_dataset_name": "name",
            "db_dataset_id": "id",
        }
    )
    return steps_df_grapher, grapher_changes


@st.cache_data(show_spinner=False)
def load_latest_population():
    # NOTE: The "channels" parameter of the find function is not working well.
    candidates = find("population", channels=("grapher",), dataset="population", namespace="demography").sort_values(
        "version", ascending=False
    )
    population = (
        candidates[(candidates["table"] == "population") & (candidates["channel"] == "grapher")]
        .iloc[0]
        .load()
        .reset_index()[["country", "year", "population"]]
    ).rename(columns={"country": "entity_name"}, errors="raise")

    return population


@st.cache_data
def get_tailscale_ip_to_user_map():
    """Get the mapping of Tailscale IPs to github usernames."""
    proc = subprocess.run(["tailscale", "status", "--json"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if proc.returncode != 0:
        log.warning(f"Error getting Tailscale status: {proc.stderr}")
        return {}

    status = json.loads(proc.stdout)
    ip_to_user = {}

    # Map user IDs to display names
    user_id_to_name = {}
    for user_id, user_info in status.get("User", {}).items():
        if "LoginName" in user_info:
            user_id_to_name[int(user_id)] = user_info["LoginName"]

    # Map IPs to user display names
    for peer in status.get("Peer", {}).values():
        user_id = peer.get("UserID")
        login_name = user_id_to_name.get(user_id)
        if login_name:
            for ip in peer.get("TailscaleIPs", []):
                ip_to_user[ip] = login_name

    return ip_to_user


@st.cache_data
def get_grapher_user_id(user_ip: str) -> Optional[int]:
    """Get the Grapher user ID associated with the given Tailscale IP address."""
    # Get Tailscale IP-to-User mapping
    ip_to_user_map = get_tailscale_ip_to_user_map()

    # Get the Tailscale display name / github username associated with the client's IP address
    github_user_name = ip_to_user_map.get(user_ip)

    if not github_user_name:
        return None

    with Session(get_engine()) as session:
        grapher_user = gm.User.load_user(session, github_user_name)

    if grapher_user:
        return grapher_user.id
    else:
        return None

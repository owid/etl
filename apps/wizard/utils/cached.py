from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from apps.utils.map_datasets import get_grapher_changes
from etl import grapher_io as gio
from etl.config import OWID_ENV, OWIDEnv
from etl.git_helpers import get_changed_files
from etl.grapher_model import Variable
from etl.version_tracker import VersionTracker


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
    dataset_uri: List[str],
    _owid_env: OWIDEnv = OWID_ENV,
) -> List[Variable]:
    """Load Variable objects that belong to a dataset with URI `dataset_uri`."""
    return gio.load_variables_in_dataset(dataset_uri, _owid_env)


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


@st.cache_data(show_spinner=False)
def get_datasets_from_version_tracker() -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
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

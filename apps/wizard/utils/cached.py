from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session

from etl import grapher_io as gio
from etl.config import OWID_ENV, OWIDEnv
from etl.grapher_model import Anomaly, Variable


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


@st.cache_data
def load_anomalies_in_dataset(
    dataset_ids: List[int],
    _owid_env: OWIDEnv = OWID_ENV,
) -> List[Anomaly]:
    """Load Anomaly objects that belong to a dataset with URI `dataset_uri`."""
    with Session(_owid_env.engine) as session:
        return Anomaly.load_anomalies(session, dataset_ids)

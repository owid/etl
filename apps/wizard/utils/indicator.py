from typing import Any, Dict, List, Optional, cast

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session

from etl.config import OWID_ENV, OWIDEnv
from etl.grapher_model import Variable


def load_indicator_uris_from_db(dataset_uris: List[str]) -> List[Variable]:
    with Session(OWID_ENV.engine) as session:
        indicators = Variable.load_variables_in_datasets(session, dataset_uris)

    # indicators = [i["catalogPath"] for i in indicators]
    # return list(indicators)
    return indicators


@st.cache_data
def load_variable_from_id(variable_id: int, _owid_env: OWIDEnv = OWID_ENV):
    with Session(_owid_env.engine) as session:
        variable = Variable.load_variable(session=session, variable_id=variable_id)

    return variable


@st.cache_data
def load_variable_from_catalog_path(catalog_path: str, _owid_env: OWIDEnv = OWID_ENV):
    with Session(_owid_env.engine) as session:
        variable = Variable.load_from_catalog_path(session=session, catalog_path=catalog_path)

    return variable


@st.cache_data
def load_variable_metadata_cached(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    _owid_env: OWIDEnv = OWID_ENV,
) -> Dict[str, Any]:
    return load_variable_metadata(
        catalog_path=catalog_path,
        variable_id=variable_id,
        variable=variable,
        owid_env=_owid_env,
    )


def load_variable_metadata(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> Dict[str, Any]:
    """Get metadata for an indicator based on its catalog path.

    Parameters
    ----------
    catalog_path : str, optional
        The path to the indicator in the catalog.
    variable_id : int, optional
        The ID of the indicator.
    variable : Variable, optional
        The indicator object.
    """
    if (catalog_path is None) and (variable_id is None) and (variable is None):
        raise ValueError("Either catalog_path, variable_id or variable must be provided")

    # Get variable
    if variable is None:
        variable = load_variable(catalog_path=catalog_path, variable_id=variable_id, owid_env=owid_env)

    variable = cast(Variable, variable)

    metadata = variable.get_metadata()

    return metadata


@st.cache_data
def load_variable_data_cached(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    _owid_env: OWIDEnv = OWID_ENV,
) -> pd.DataFrame:
    return load_variable_data(
        catalog_path=catalog_path,
        variable_id=variable_id,
        variable=variable,
        owid_env=_owid_env,
    )


def load_variable_data(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> pd.DataFrame:
    """Get data for an indicator based on its catalog path.

    Parameters
    ----------
    cataslog_path : str, optional
        The path to the indicator in the catalog.
    variable_id : int, optional
        The ID of the indicator.
    variable : Variable, optional
        The indicator object.

    """
    if (catalog_path is None) and (variable_id is None) and (variable is None):
        raise ValueError("Either catalog_path, variable_id or variable must be provided")

    # Get variable
    if variable is None:
        variable = load_variable(catalog_path=catalog_path, variable_id=variable_id, owid_env=owid_env)

    # Get data
    with Session(owid_env.engine) as session:
        df = variable.get_data(session=session)

    return df


def load_variable(catalog_path: Optional[str] = None, variable_id: Optional[int] = None, owid_env: OWIDEnv = OWID_ENV):
    if catalog_path is not None:
        variable = load_variable_from_catalog_path(catalog_path=catalog_path, _owid_env=owid_env)
    else:
        if variable_id is not None:
            variable = load_variable_from_id(variable_id=variable_id, _owid_env=owid_env)
        else:
            raise ValueError("Either catalog_path or variable_id must be provided")
    return variable

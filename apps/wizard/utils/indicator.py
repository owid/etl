from typing import Any, Dict, List, Optional, cast

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session

from etl.config import OWID_ENV, OWIDEnv
from etl.grapher_model import Variable


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


def load_indicator_uris_from_db(dataset_uris: List[str]) -> List[Variable]:
    with Session(OWID_ENV.engine) as session:
        indicators = Variable.load_variables_in_datasets(session, dataset_uris)

    # indicators = [i["catalogPath"] for i in indicators]
    # return list(indicators)
    return indicators


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

    # Get metadata
    metadata = variable.get_metadata()

    return metadata


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


def load_variable(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> Variable:
    """Load variable"""
    with Session(owid_env.engine) as session:
        variable = Variable.from_db(
            session=session,
            catalog_path=catalog_path,
            variable_id=variable_id,
        )

    variable = cast(Variable, variable)

    return variable

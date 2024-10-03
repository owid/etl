from typing import Any, Dict, Optional, cast

import pandas as pd
import requests
import streamlit as st
from sqlalchemy.orm import Session

from etl.config import OWID_ENV
from etl.grapher_model import Variable


@st.cache_data
def load_variable_from_id(variable_id: int):
    with Session(OWID_ENV.engine) as session:
        variable = Variable.load_variable(session=session, variable_id=variable_id)

    return variable


@st.cache_data
def load_variable_from_catalog_path(catalog_path: str):
    with Session(OWID_ENV.engine) as session:
        variable = Variable.load_from_catalog_path(session=session, catalog_path=catalog_path)

    return variable


@st.cache_data
def load_variable_metadata(variable: Variable) -> Dict[str, Any]:
    metadata = requests.get(variable.s3_metadata_path(typ="http")).json()

    return metadata


@st.cache_data
def load_variable_data(catalog_path: Optional[str] = None, variable: Optional[Variable] = None) -> pd.DataFrame:
    """TODO: add country names. look into /home/lucas/repos/etl/apps/backport/datasync/data_metadata.py:add_entity_code_and_name"""
    if catalog_path is None and variable is None:
        raise ValueError("Either catalog_path or variable must be provided")

    if catalog_path is not None:
        variable = load_variable_from_catalog_path(catalog_path=catalog_path)

    variable = cast(Variable, variable)

    data = requests.get(variable.s3_data_path(typ="http")).json()
    df = pd.DataFrame(data)

    return df

from typing import Any, Dict, List, Optional, cast

import pandas as pd
import requests
import streamlit as st
from sqlalchemy.orm import Session

from etl.config import OWID_ENV
from etl.db import read_sql
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
def load_variable_data(
    catalog_path: Optional[str] = None,
    variable: Optional[Variable] = None,
    col_entity_name: Optional[str] = "entity",
) -> pd.DataFrame:
    """Get data for an indicator based on its catalog path.

    Parameters
    ----------
    cataslog_path : str, optional
        The path to the indicator in the catalog.
    variable : Variable, optional
        The indicator object.
    col_entity_name : str, optional
        The name of the column containing entity names. Set to None to keep entity codes.

    """
    if catalog_path is None and variable is None:
        raise ValueError("Either catalog_path or variable must be provided")

    if catalog_path is not None:
        variable = load_variable_from_catalog_path(catalog_path=catalog_path)

    variable = cast(Variable, variable)

    data = requests.get(variable.s3_data_path(typ="http")).json()
    df = pd.DataFrame(data)

    # Replace entity codes with entity names
    if col_entity_name is not None:
        with Session(OWID_ENV.engine) as session:
            df = add_entity_name(session=session, df=df, col_id="entities", col_name=col_entity_name)
    return df


def add_entity_name(
    session: Session,
    df: pd.DataFrame,
    col_id: str,
    col_name: str = "entity",
    col_code: Optional[str] = None,
    remove_id: bool = True,
) -> pd.DataFrame:
    # Initialize
    if df.empty:
        df[col_name] = []
        if col_code is not None:
            df[col_code] = []
        return df

    # Get entity names
    unique_entities = df[col_id].unique()
    entities = _fetch_entities(session, list(unique_entities), col_id, col_name, col_code)

    # Sanity check
    if set(unique_entities) - set(entities[col_id]):
        missing_entities = set(unique_entities) - set(entities[col_id])
        raise ValueError(f"Missing entities in the database: {missing_entities}")

    # Set dtypes
    dtypes = {col_name: "category"}
    if col_code is not None:
        dtypes[col_code] = "category"
    df = pd.merge(df, entities.astype(dtypes), on=col_id)

    # Remove entity IDs
    if remove_id:
        df = df.drop(columns=[col_id])

    return df


def _fetch_entities(
    session: Session,
    entity_ids: List[int],
    col_id: Optional[str] = None,
    col_name: Optional[str] = None,
    col_code: Optional[str] = None,
) -> pd.DataFrame:
    # Query entities from the database
    q = """
    SELECT
        id AS entityId,
        name AS entityName,
        code AS entityCode
    FROM entities
    WHERE id in %(entity_ids)s
    """
    df = read_sql(q, session, params={"entity_ids": entity_ids})

    # Rename columns
    column_renames = {}
    if col_id is not None:
        column_renames["entityId"] = col_id
    if col_name is not None:
        column_renames["entityName"] = col_name
    if col_code is not None:
        column_renames["entityCode"] = col_code
    else:
        df = df.drop(columns=["entityCode"])

    df = df.rename(columns=column_renames)
    return df

from typing import Any, Dict, List, Optional, cast

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session

from etl.config import OWID_ENV, OWIDEnv
from etl.grapher_model import Variable


def load_variables_in_dataset(dataset_uris: List[str]) -> List[Variable]:
    with Session(OWID_ENV.engine) as session:
        indicators = Variable.load_variables_in_datasets(session, dataset_uris)

    return indicators


# Load variable object
def load_variable(
    id_or_path: str | int,
    owid_env: OWIDEnv = OWID_ENV,
) -> Variable:
    """Load variable"""
    with Session(owid_env.engine) as session:
        variable = Variable.from_id_or_path(
            session=session,
            id_or_path=id_or_path,
        )

    variable = cast(Variable, variable)

    return variable


# Load variable metadata
def load_variable_metadata(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> Dict[str, Any]:
    """Get metadata for an indicator based on its catalog path or variable id.

    Parameters
    ----------
    catalog_path : str, optional
        The path to the indicator in the catalog.
    variable_id : int, optional
        The ID of the indicator.
    variable : Variable, optional
        The indicator object.
    """
    # Get variable
    variable = ensure_variable(catalog_path, variable_id, variable, owid_env)

    # Get metadata
    metadata = variable.get_metadata()

    return metadata


# Load variable data
def load_variable_data(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> pd.DataFrame:
    """Get data for an indicator based on its catalog path or variable id.

    Parameters
    ----------
    cataslog_path : str, optional
        The path to the indicator in the catalog.
    variable_id : int, optional
        The ID of the indicator.
    variable : Variable, optional
        The indicator object.

    """

    # Get variable
    variable = ensure_variable(catalog_path, variable_id, variable, owid_env)

    # Get data
    with Session(owid_env.engine) as session:
        df = variable.get_data(session=session)

    return df


def ensure_variable(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int] = None,
    variable: Optional[Variable] = None,
    owid_env: OWIDEnv = OWID_ENV,
) -> Variable:
    if variable is None:
        if catalog_path is not None:
            variable = load_variable(id_or_path=catalog_path, owid_env=owid_env)
        elif variable_id is not None:
            variable = load_variable(id_or_path=variable_id, owid_env=owid_env)
        else:
            raise ValueError("Either catalog_path, variable_id or variable must be provided")
    return variable


def variable_data_df_from_s3(
    engine: Engine,
    variable_ids: List[int] = [],
    workers: int = 1,
    value_as_str: bool = True,
) -> pd.DataFrame:
    """Fetch data from S3 and add entity code and name from DB."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(_fetch_data_df_from_s3, variable_ids))

    if isinstance(results, list) and all(isinstance(df, pd.DataFrame) for df in results):
        df = pd.concat(cast(List[pd.DataFrame], results))
    else:
        raise TypeError(f"results must be a list of pd.DataFrame, got {type(results)}")

    # we work with strings and convert to specific types later
    if value_as_str:
        df["value"] = df["value"].astype("string")

    with Session(engine) as session:
        res = add_entity_code_and_name(session, df)
        return res

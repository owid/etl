"""Translate the variable mapping dictionary from environment 1 to environment 2.


That is, the variable IDs for the same variables may differ between environments (local, staging or production environments).
If you have the variable mapping for one of the environments, you can easily obtain the equivalent for another environment using this command.

A common use case is when you have the mapping for your local environment and wish to have the equivalent for the production environment. Instead
of creating yet again the mapping for the production environment, simply run this command which will 'translate' the mapping you found for your
local environment to one that is consistent with the production environment's variable IDs.
"""
import json
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import pandas as pd
import rich_click as click
import structlog
from dotenv import dotenv_values
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

log = structlog.get_logger()


@click.command(help=__doc__)
@click.option(
    "-e1",
    "--env-file-1",
    type=str,
    help=(
        "Path to the configuration file for connection 1. This file should contain all the environment variables"
        " required to connect to the SQL. Should be in the format of a .env file."
    ),
    required=True,
)
@click.option(
    "-e2",
    "--env-file-2",
    type=str,
    help=(
        "Path to the configuration file for connection 2. This file should contain all the environment variables"
        " required to connect to the SQL. Should be in the format of a .env file."
    ),
    required=True,
)
@click.option(
    "-m1",
    "--mapping-file-1",
    type=str,
    help=(
        "Path to the JSON file containing the variable mapping from connection 1. This file should have been previously"
        " created and curated by the user. See command `etlcli variable-match` to create this file."
    ),
    required=True,
)
@click.option(
    "-m2",
    "--mapping-file-2",
    type=str,
    help="Path where to store the newly generated variable mapping from connection 2.",
    required=True,
)
def main_cli(env_file_1: str, env_file_2: str, mapping_file_1: str, mapping_file_2: str) -> None:
    """Generate equivalent variable mapping file for the new DB.

    Parameters
    ----------
        env_1_file: str
            Path to environment file with configuration to connect to database 1 (aka old DB).
        env_file_2: str
            Path to environment file with configuration to connect to database 2 (aka new DB).
        mapping_file_1: str
            Path to JSON file with variable IDs mapping (according to database 1)
        mapping_file_2: str
            Path to JSON file with variable IDs mapping (according to database 2)
    """
    var_translator = VariableMappingTranslate.from_files(
        config_file_1=env_file_1,
        config_file_2=env_file_2,
        mapping_file_1=mapping_file_1,
    )
    # Get new mapping
    mapping_2 = var_translator.translate()
    # Export
    log.info(f"Writing new mapping to file {mapping_file_2}...")
    _write_mapping(mapping_2, mapping_file_2)
    log.info("Done.")


@dataclass
class VariableMappingTranslate:
    config_connection_1: Dict[str, str]
    config_connection_2: Dict[str, str]
    mapping_1: Dict[str, str]

    @classmethod
    def from_files(cls, config_file_1: str, config_file_2: str, mapping_file_1: str) -> "VariableMappingTranslate":
        """Read class attributes from files."""
        config_connection_1: Dict[str, str] = _read_vars_from_env(config_file_1)
        config_connection_2: Dict[str, str] = _read_vars_from_env(config_file_2)
        mapping_1: Dict[str, str] = _read_mapping(mapping_file_1)
        return cls(config_connection_1, config_connection_2, mapping_1)

    def translate(self) -> Dict[str, str]:
        # Build engines
        log.info("Building engines to connect to database 1...")
        eng_1 = _build_engine(self.config_connection_1)
        log.info("Building engines to connect to database 2...")
        eng_2 = _build_engine(self.config_connection_2)
        # Get new mapping
        mapping_2 = variable_mapping_translate(eng_1, eng_2, self.mapping_1)
        return mapping_2


def _write_mapping(mapping: Dict[str, str], mapping_file: str) -> None:
    """Write mapping dictionary to disk as a JSON file."""
    with open(mapping_file, "w") as f:
        json.dump(mapping, f, indent=2)


def _read_mapping(mapping_file: str) -> Dict[str, str]:
    """Read mapping dictionary from disk (JSON file)."""
    with open(mapping_file, "r") as f:
        mapping: Dict[str, str] = json.load(f)
    return mapping


def _read_vars_from_env(path: str) -> Dict[str, Any]:
    """Read environment variables from .env file."""
    config = dotenv_values(path)
    return {
        "db": config["DB_NAME"],
        "host": config["DB_HOST"],
        "port": int(str(config["DB_PORT"])),
        "user": config["DB_USER"],
        "password": config.get("DB_PASS", ""),
    }


def _build_engine(conf: Dict[str, str]) -> Engine:
    """Build SQL connection object"""
    return create_engine("mysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4".format(**conf))


def variable_mapping_translate(sql_1: Engine, sql_2: Engine, mapping: Dict[str, str]) -> Dict[str, str]:
    """Obtain equivalent variable mapping from old to new DB.

    Parameters
    ----------
        conn_1: Engine)
            Connection to database 1 (aka old DB).
        conn_2: Engine)
            Connection to database 2 (aka new DB).
        mapping: dict
            Dictionary with variable IDs mapping (according to database 1)
    Returns
    -------
        dict:
            Dictionary with variable IDs mapping according to database 2.
    """
    # Get info from connection 1
    log.info("Getting variable details from database 1...")
    df = build_df_from_mapping(sql_1, mapping)
    # Get ids from connection 2
    log.info("Get IDs from database 2...")
    mapping_new = build_mapping_from_df(sql_2, df)
    return mapping_new


def build_df_from_mapping(sql: Engine, mapping: Dict[str, str]) -> pd.DataFrame:
    """Build dataframe from variable ids.

    Contains variable id, variable name and dataset name for the old and new variables.

    Parameters
    ----------
    mapping: dict
        Dictionary mapping old to new variable ids.

    Returns
    -------
    pandas.DataFrame:
        Dataframe with all variable info.
    """
    df_old, df_new = _build_dfs(sql, mapping)
    df = _merge_dfs(df_old, df_new, mapping)
    return df


def _run_query_mapping_to_df(sql: Engine, variable_ids: Tuple[str, ...]) -> pd.DataFrame:
    """Get complete variable df from variable ids.

    Parameters
    ----------
    sql : Engine
        DB connection object.
    variable_ids: list
        List with variable ids.

    Returns
    -------
    pandas.DataFrame :
        Dataframe with variable fields: `id`, `name` and `dataset_name`.
    """
    query = """
        select variables.id id, variables.name name, datasets.name dataset_name
        from variables
        left join datasets on variables.datasetId=datasets.id
        where variables.id in %(variable_ids)s;
    """
    df: pd.DataFrame = pd.read_sql_query(query, sql, params={"variable_ids": variable_ids})
    return df


def _build_dfs(sql: Engine, mapping: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build dataframes for old and new variable ids using mapping."""
    ids_old: Tuple[str, ...] = tuple(mapping.keys())
    ids_new: Tuple[str, ...] = tuple(mapping.values())
    df_old = _run_query_mapping_to_df(sql, ids_old)
    df_new = _run_query_mapping_to_df(sql, ids_new)
    return df_old, df_new


def _merge_dfs(df_old: pd.DataFrame, df_new: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """Merge old and new dataframes into single dataframe."""
    df_old = df_old.rename(columns={col: f"{col}_old" for col in df_old.columns})
    df_new = df_new.rename(columns={col: f"{col}_new" for col in df_new.columns})
    df = df_old.assign(id_new=df_old.id_old.astype(str).map(mapping).astype(int))
    df = df.merge(df_new, on="id_new")
    return df


def build_mapping_from_df(sql: Engine, df: pd.DataFrame) -> Dict[Any, Any]:
    """_summary_

    Parameters
    ----------
        sql: Engine
            DB connection object.
        df: pd.DataFrame
            DataFrame with variable info.

    Returns
    -------
        dict: New variable mapping dictionary.
    """
    # Get old and new data from DB
    df_old = _build_individual_df(sql, df, "name_old", "dataset_name_old")
    df_new = _build_individual_df(sql, df, "name_new", "dataset_name_new")
    # Merge into single df
    df = _merge_dfs_old_new(df_old, df_new)
    dix: Dict[Any, Any] = df.set_index("id_old")["id_new"].to_dict()
    return dix


def _build_individual_df(sql: Engine, df: pd.DataFrame, column_name: str, column_dataset_name: str) -> pd.DataFrame:
    var_names: Tuple[str, ...] = tuple(df[column_name].tolist())
    ds_names: Tuple[str, ...] = tuple(df[column_dataset_name].tolist())
    df_ = _get_partial_df(sql, var_names, ds_names)

    # sort
    df_ = df.merge(df_, left_on=[column_name, column_dataset_name], right_on=["name", "dataset_name"])[
        ["id", "name", "dataset_name"]
    ]
    # sanity check
    assert len(df) == len(df_)
    assert df_.isna().sum().sum() == 0
    return df_


def _merge_dfs_old_new(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    df = pd.concat(
        [
            df_old.rename(columns={col: f"{col}_old" for col in df_old.columns}),
            df_new.rename(columns={col: f"{col}_new" for col in df_new.columns}),
        ],
        axis=1,
    ).astype(str)
    return df


def _get_partial_df(sql: Engine, var_names: Tuple[str, ...], ds_names: Tuple[str, ...]) -> pd.DataFrame:
    names = tuple(f"{v}_{d}" for v, d in zip(var_names, ds_names))
    query = """
        select variables.id id, variables.name name, datasets.name dataset_name from variables
        left join datasets on variables.datasetId = datasets.id
        where CONCAT(variables.name, '_', datasets.name) in %(names)s;
    """
    df: pd.DataFrame = pd.read_sql_query(query, sql, params={"names": names})
    return df


def _sanity_check(sql: Engine, mapping: Dict[str, str]) -> None:
    # build df from local mapping
    df = build_df_from_mapping(sql, mapping)
    # build mapping from names
    mapping_2 = build_mapping_from_df(sql, df)

    # local check
    assert set(mapping_2.keys()) == set(mapping.keys())
    assert set(mapping_2.values()) == set(mapping.values())

    # successfull message
    log.info("Sanity check passed.")


if __name__ == "__main__":
    main_cli()

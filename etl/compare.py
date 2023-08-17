#
#  compare.py
#

from pathlib import Path
from typing import Any, Dict, Optional, Union, cast
from urllib.parse import quote

import click
import pandas as pd
import rich
from dotenv import dotenv_values
from owid import catalog
from owid.repack import repack_frame
from rich import print
from rich_click.rich_command import RichCommand
from rich_click.rich_group import RichGroup
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from backport.datasync.data_metadata import variable_data_df_from_s3
from etl import tempcompare


@click.group(cls=RichGroup)
@click.option(
    "--absolute-tolerance",
    default=0.00000001,
    show_default=True,
    help="The absolute tolerance for floating point comparisons.",
)
@click.option(
    "--relative-tolerance",
    default=0.05,
    show_default=True,
    help="The relative tolerance for floating point comparisons.",
)
@click.option(
    "--show-values/--hide-values",
    default=False,
    show_default=True,
    help="Show a preview of the values where the dataframes are different.",
)
@click.option(
    "--show-shared/--hide-shared",
    default=False,
    show_default=True,
    help="Show the structural overlap of the two dataframes (shared columns, index columns and index values).",
)
@click.option(
    "--truncate-lists-at",
    default=20,
    show_default=True,
    help="Print truncated lists if they are longer than the given length.",
)
@click.pass_context
def cli(
    ctx: click.core.Context,
    absolute_tolerance: float,
    relative_tolerance: float,
    show_values: bool,
    show_shared: bool,
    truncate_lists_at: int,
) -> None:
    """Compare two dataframes/tables/datasets in terms of their structure, values and metadata."""
    ctx.ensure_object(dict)
    ctx.obj["absolute_tolerance"] = absolute_tolerance
    ctx.obj["relative_tolerance"] = relative_tolerance
    ctx.obj["show_values"] = show_values
    ctx.obj["show_shared"] = show_shared
    ctx.obj["truncate_lists_at"] = truncate_lists_at


def diff_print(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df1_label: str,
    df2_label: str,
    absolute_tolerance: float,
    relative_tolerance: float,
    show_values: bool,
    show_shared: bool,
    truncate_lists_at: int,
    print: Any = rich.print,
) -> int:
    """Runs the comparison and prints the differences, then return exit code."""
    diff = tempcompare.HighLevelDiff(df1, df2, absolute_tolerance, relative_tolerance)
    if diff.are_equal:
        print("[green]Dataframes are equal (within the given thresholds)[/green]")
        return 0
    else:
        lines = diff.get_description_lines_for_diff(
            df1_label,
            df2_label,
            use_color_tags=True,
            preview_different_dataframe_values=show_values,
            show_shared=show_shared,
            truncate_lists_longer_than=truncate_lists_at,
        )
        for line in lines:
            print(line)
        if diff.are_structurally_equal:
            return 2
        else:
            return 3


@cli.command(cls=RichCommand)
@click.argument("channel")
@click.argument("namespace")
@click.argument("dataset")
@click.argument("table")
@click.option("--version", default=None, help="Version of catalog dataset to compare with.")
@click.option("--debug", is_flag=True, help="Print debug information.")
@click.pass_context
def etl_catalog(
    ctx: click.core.Context,
    channel: str,
    namespace: str,
    dataset: str,
    table: str,
    version: Optional[Union[str, int]],
    debug: bool,
) -> None:
    """
    Compare a table in the local catalog with the analogous one in the remote catalog.

    If "version" is not specified, the latest local version of the dataset is compared with the latest remote version of
    the same dataset. To impose a specific version of both the local and remote datasets, use use the "version"
    optional argument, e.g. --version "2022-01-01".

    It compares the columns, index columns and index values (row indices) as sets between the two dataframes and outputs
    the differences. Finally it compares the values of the overlapping columns and rows with the given threshold values
    for absolute and relative tolerance.

    The exit code is 0 if the dataframes are equal, 1 if there is an error loading the dataframes, 2 if the dataframes
    are structurally equal but are otherwise different, 3 if the dataframes have different structure and/or different values.
    """
    try:
        remote_df = catalog.find_latest(
            table=table, namespace=namespace, dataset=dataset, channels=[channel], version=version
        )
    except Exception as e:
        if debug:
            raise e
        print(f"[red]Error loading table from remote catalog:\n{e}[/red]")
        exit(1)

    try:
        local_catalog = catalog.LocalCatalog("data")
        local_catalog.reindex(table)
        try:
            local_df = local_catalog.find_latest(
                table=table,
                namespace=namespace,
                dataset=dataset,
                channel=cast(catalog.CHANNEL, channel),
                version=version,
            )
        except ValueError as e:
            # try again after reindexing
            if str(e) == "no tables found":
                local_catalog.reindex(include=f"{channel}/{namespace}")
                local_df = local_catalog.find_latest(
                    table=table,
                    namespace=namespace,
                    dataset=dataset,
                    channel=cast(catalog.CHANNEL, channel),
                    version=version,
                )
            else:
                raise e
    except Exception as e:
        if debug:
            raise e
        print(f"[red]Error loading table from local catalog:\n{e}[/red]")
        exit(1)

    return_code = diff_print(
        remote_df,
        local_df,
        "remote",
        "local",
        **ctx.obj,
    )
    exit(return_code)


@cli.command(cls=RichCommand)
@click.argument("namespace")
@click.argument("version")
@click.argument("dataset")
@click.option(
    "--remote-env",
    type=click.Path(exists=True),
    help="Path to .env file with remote database credentials.",
    default=".env.prod",
)
@click.option(
    "--local-env",
    type=click.Path(exists=True),
    help="Path to .env file with remote database credentials.",
    default=".env",
)
@click.option("--values", is_flag=True, help="Compare values from S3.")
@click.pass_context
def grapher(
    ctx: click.core.Context,
    namespace: str,
    version: str,
    dataset: str,
    remote_env: str,
    local_env: str,
    values: bool,
) -> None:
    """
    Compare a dataset in the local database with the remote database.

    It compares dataset and variables metadata, and optionally the values from S3 with --values flag
    (which can be both CPU and memory heavy). It does the comparison in the same way as the etl-catalog command.

    The exit code is always 0 even if dataframes are different.

    Example usage:
        compare  --show-values grapher ggdc 2020-10-01 ggdc_maddison__2020_10_01 --values
    """
    remote_dataset_df = read_dataset_from_db(remote_env, namespace, version, dataset)
    local_dataset_df = read_dataset_from_db(local_env, namespace, version, dataset)

    print("[magenta]=== Comparing dataset ===[/magenta]")
    diff_print(
        remote_dataset_df,
        local_dataset_df,
        "remote",
        "local",
        **ctx.obj,
    )

    remote_variables_df = read_variables_from_db(remote_env, namespace, version, dataset)
    local_variables_df = read_variables_from_db(local_env, namespace, version, dataset)

    # use preferably shortName as index or name if shortName is missing
    if remote_variables_df.shortName.notnull().all() and local_variables_df.shortName.notnull().all():
        index_name = "shortName"
    else:
        index_name = "name"
    remote_variables_df = remote_variables_df.set_index(index_name)
    local_variables_df = local_variables_df.set_index(index_name)

    print("\n[magenta]=== Comparing variables ===[/magenta]")
    diff_print(
        remote_variables_df,
        local_variables_df,
        "remote",
        "local",
        **ctx.obj,
    )

    remote_sources_df = read_sources_from_db(remote_env, namespace, version, dataset)
    local_sources_df = read_sources_from_db(local_env, namespace, version, dataset)

    print("\n[magenta]=== Comparing sources ===[/magenta]")
    diff_print(
        remote_sources_df,
        local_sources_df,
        "remote",
        "local",
        **ctx.obj,
    )

    if values:
        remote_values_df = read_values_from_s3(remote_env, namespace, version, dataset)
        local_values_df = read_values_from_s3(local_env, namespace, version, dataset)

        print("\n[magenta]=== Comparing values ===[/magenta]")
        diff_print(
            remote_values_df,
            local_values_df,
            "remote",
            "local",
            **ctx.obj,
        )


def get_engine(config: Dict[str, Any]) -> Engine:
    return create_engine(
        f'mysql://{config["DB_USER"]}:{quote(config["DB_PASS"])}@{config["DB_HOST"]}:{config["DB_PORT"]}/{config["DB_NAME"]}'
    )


def read_dataset_from_db(env_path: str, namespace: str, version: str, dataset: str) -> pd.DataFrame:
    engine = get_engine(dotenv_values(env_path))

    q = """
    SELECT * FROM datasets
    WHERE version = %(version)s and namespace = %(namespace)s and shortName = %(dataset)s
    """

    df = pd.read_sql(
        q,
        engine,
        params={"version": version, "namespace": namespace, "dataset": dataset},
    )

    # drop uninteresting columns
    df = df.drop(["createdByUserId", "dataEditedAt", "metadataEditedAt", "updatedAt"], axis=1)

    return cast(pd.DataFrame, df)


def read_variables_from_db(env_path: str, namespace: str, version: str, dataset: str) -> pd.DataFrame:
    engine = get_engine(dotenv_values(env_path))

    q = """
    SELECT
        variables.*
    FROM variables
    JOIN datasets as d ON variables.datasetId = d.id
    WHERE d.version = %(version)s and d.namespace = %(namespace)s and d.shortName = %(dataset)s
    """

    df = pd.read_sql(
        q,
        engine,
        params={"version": version, "namespace": namespace, "dataset": dataset},
    )

    # drop uninteresting columns
    df = df.drop(["updatedAt", "createdAt", "catalogPath"], axis=1)

    return cast(pd.DataFrame, df)


def read_sources_from_db(env_path: str, namespace: str, version: str, dataset: str) -> pd.DataFrame:
    engine = get_engine(dotenv_values(env_path))

    # compare only variables sources, we are not using dataset sources for anything
    q = """
    SELECT distinct
        sources.*
    FROM sources
    JOIN variables ON sources.id = variables.sourceId
    JOIN datasets as d ON variables.datasetId = d.id
    WHERE d.version = %(version)s and d.namespace = %(namespace)s and d.shortName = %(dataset)s
    """

    df = pd.read_sql(
        q,
        engine,
        params={"version": version, "namespace": namespace, "dataset": dataset},
    )

    # drop uninteresting columns
    df = df.drop(["updatedAt", "createdAt"], axis=1)

    return cast(pd.DataFrame, df)


def read_values_from_s3(env_path: str, namespace: str, version: str, dataset: str) -> pd.DataFrame:
    engine = get_engine(dotenv_values(env_path))

    # get variables
    q = """
    SELECT
        v.id as variableId,
        v.name as variable
    FROM variables as v
    JOIN datasets as d ON v.datasetId = d.id
    WHERE d.version = %(version)s and d.namespace = %(namespace)s and d.shortName = %(dataset)s
    """
    vf = pd.read_sql(
        q,
        engine,
        params={"version": version, "namespace": namespace, "dataset": dataset},
    )

    # read them from S3
    df = variable_data_df_from_s3(engine, variable_ids=vf.variableId.tolist(), workers=10)

    # add variable name
    df = df.merge(vf[["variableId", "variable"]], on="variableId")

    # pivot table for easier comparison
    df = df.pivot(index=["year", "entityId"], columns="variable", values="value")
    df = df.sort_index(axis=0).sort_index(axis=1)

    df = repack_frame(df.reset_index()).set_index(["year", "entityId"])

    return cast(pd.DataFrame, df)


def load_table(path_str: str) -> catalog.Table:
    """Loads a Table (dataframe + metadata) from a path."""
    path = Path(path_str)
    if not path.exists():
        raise Exception("File does not exist: " + path_str)

    if path.suffix.lower() == ".feather":
        return catalog.tables.Table.read_feather(path_str)
    elif path.suffix.lower() == ".csv":
        return catalog.tables.Table.read_csv(path_str)
    else:
        raise Exception("Unknown file format: " + path_str)


def load_dataframe(path_str: str) -> pd.DataFrame:
    """Loads a DataFrame from a path."""
    path = Path(path_str)
    if not path.exists():
        raise Exception("File does not exist: " + path_str)

    if path.suffix.lower() == ".feather":
        return cast(pd.DataFrame, pd.read_feather(path_str))
    elif path.suffix.lower() == ".csv":
        return pd.read_csv(path_str)
    elif path.suffix.lower() == ".parquet":
        return cast(pd.DataFrame, pd.read_parquet(path_str))
    else:
        raise Exception("Unknown file format: " + path_str)


@cli.command(cls=RichCommand)
@click.argument("dataframe1")
@click.argument("dataframe2")
@click.pass_context
def dataframes(
    ctx: click.core.Context,
    dataframe1: str,
    dataframe2: str,
) -> None:
    """
    Compare two dataframes given as paths.

    It compares the columns, index columns and index values (row indices) as
    sets between the two dataframes and outputs the differences. Finally it compares the values of the overlapping
    columns and rows with the given threshold values for absolute and relative tolerance.

    The exit code is 0 if the dataframes are equal, 1 if there is an error loading the dataframes, 2 if the dataframes
    are structurally equal but are otherwise different, 3 if the dataframes have different structure and/or different values.
    """
    df1: pd.DataFrame
    df2: pd.DataFrame
    print("🦸 OWID's friendly dataframe comparision tool - at your service! 🦸")
    try:
        df1 = load_table(dataframe1)
    except Exception:
        print(f"[yellow]Reading {dataframe1} as table with metadata failed, trying to read as plain dataframe[/yellow]")
        try:
            df1 = load_dataframe(dataframe1)
        except Exception as e:
            print(f"[red]Reading {dataframe1} as dataframe failed[/red]")
            print(e)
            exit(1)

    try:
        df2 = load_table(dataframe2)
    except Exception:
        print(f"[yellow]Reading {dataframe2} as table with metadata failed, trying to read as plain dataframe[/yellow]")
        try:
            df2 = load_dataframe(dataframe2)
        except Exception as e:
            print(f"[red]Reading {dataframe2} as dataframe failed[/red]")
            print(e)
            exit(1)

    return_code = diff_print(
        df1,
        df2,
        "dataframe1",
        "dataframe2",
        **ctx.obj,
    )
    exit(return_code)


if __name__ == "__main__":
    cli()

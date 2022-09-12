#
#  compare.py
#

from pathlib import Path
from typing import Any, Dict, cast
from urllib.parse import quote

import click
import pandas as pd
from dotenv import dotenv_values
from owid import catalog
from owid.catalog.frames import repack_frame
from rich import print
from rich_click.rich_command import RichCommand
from rich_click.rich_group import RichGroup
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from etl import tempcompare


@click.group(cls=RichGroup)
def cli() -> None:
    """Compare two dataframes, both structurally and the values.

    This tool loads two dataframes, either from the local ETL and the remote catalog
    or just from two different files. It compares the columns, index columns and index values (row indices) as
    sets between the two dataframes and outputs the differences. Finally it compares the values of the overlapping
    columns and rows with the given threshold values for absolute and relative tolerance.

    The exit code is 0 if the dataframes are equal, 1 if there is an error loading the dataframes, 2 if the dataframes
    are structurally equal but are otherwise different, 3 if the dataframes have different structure and/or different values.
    """
    pass


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
@click.option("--debug", is_flag=True, help="Print debug information.")
def etl_catalog(
    channel: str,
    namespace: str,
    dataset: str,
    table: str,
    absolute_tolerance: float,
    relative_tolerance: float,
    show_values: bool,
    show_shared: bool,
    truncate_lists_at: int,
    debug: bool,
) -> None:
    """
    Compare a table in the local catalog with the one in the remote catalog.

    It compares the columns, index columns and index values (row indices) as
    sets between the two dataframes and outputs the differences. Finally it compares the values of the overlapping
    columns and rows with the given threshold values for absolute and relative tolerance.

    The exit code is 0 if the dataframes are equal, 1 if there is an error loading the dataframes, 2 if the dataframes
    are structurally equal but are otherwise different, 3 if the dataframes have different structure and/or different values.
    """
    try:
        remote_df = catalog.find_one(table=table, namespace=namespace, dataset=dataset, channels=[channel])
    except Exception as e:
        if debug:
            raise e
        print(f"[red]Error loading table from remote catalog:\n{e}[/red]")
        exit(1)

    try:
        local_catalog = catalog.LocalCatalog("data")
        try:
            local_df = local_catalog.find_one(
                table=table,
                namespace=namespace,
                dataset=dataset,
                channel=cast(catalog.CHANNEL, channel),
            )
        except ValueError as e:
            # try again after reindexing
            if str(e) == "no tables found":
                local_catalog.reindex(include=f"{channel}/{namespace}")
                local_df = local_catalog.find_one(
                    table=table,
                    namespace=namespace,
                    dataset=dataset,
                    channel=cast(catalog.CHANNEL, channel),
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
        absolute_tolerance,
        relative_tolerance,
        show_values,
        show_shared,
        truncate_lists_at,
    )
    exit(return_code)


@cli.command(cls=RichCommand)
@click.argument("namespace")
@click.argument("version")
@click.argument("dataset")
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
@click.option(
    "--remote-env",
    type=click.Path(exists=True),
    help="Path to .env file with remote database credentials.",
    default=".env.production",
)
@click.option(
    "--local-env",
    type=click.Path(exists=True),
    help="Path to .env file with remote database credentials.",
    default=".env",
)
@click.option("--data-values", is_flag=True, help="Compare data_values table.")
def grapher(
    namespace: str,
    version: str,
    dataset: str,
    absolute_tolerance: float,
    relative_tolerance: float,
    show_values: bool,
    show_shared: bool,
    truncate_lists_at: int,
    remote_env: str,
    local_env: str,
    data_values: bool,
) -> None:
    """
    Compare a dataset in the local database with the remote database.

    It compares dataset and variables metadata, and optionally the data_values table with --data-values flag
    (which can be both CPU and memory heavy). It does the comparison in the same way as the etl-catalog command.

    The exit code is always 0 even if dataframes are different.

    Example usage:
        compare grapher ggdc 2020-10-01 ggdc_maddison__2020_10_01 --show-values --data-values
    """
    remote_dataset_df = read_dataset_from_db(remote_env, namespace, version, dataset)
    local_dataset_df = read_dataset_from_db(local_env, namespace, version, dataset)

    print("[magenta]=== Comparing dataset ===[/magenta]")
    diff_print(
        remote_dataset_df,
        local_dataset_df,
        "remote",
        "local",
        absolute_tolerance,
        relative_tolerance,
        show_values,
        show_shared,
        truncate_lists_at,
    )

    remote_variables_df = read_variables_from_db(remote_env, namespace, version, dataset)
    local_variables_df = read_variables_from_db(local_env, namespace, version, dataset)

    print("\n[magenta]=== Comparing variables ===[/magenta]")
    diff_print(
        remote_variables_df,
        local_variables_df,
        "remote",
        "local",
        absolute_tolerance,
        relative_tolerance,
        show_values,
        show_shared,
        truncate_lists_at,
    )

    if data_values:
        remote_data_values_df = read_data_values_from_db(remote_env, namespace, version, dataset)
        local_data_values_df = read_data_values_from_db(local_env, namespace, version, dataset)

        __import__("ipdb").set_trace()

        print("\n[magenta]=== Comparing data_values ===[/magenta]")
        diff_print(
            remote_data_values_df,
            local_data_values_df,
            "remote",
            "local",
            absolute_tolerance,
            relative_tolerance,
            show_values,
            show_shared,
            truncate_lists_at,
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
    df = df.drop(["updatedAt"], axis=1)

    return cast(pd.DataFrame, df)


def read_data_values_from_db(env_path: str, namespace: str, version: str, dataset: str) -> pd.DataFrame:
    engine = get_engine(dotenv_values(env_path))

    q = """
    SELECT
        dv.*,
        v.name as variable
    FROM data_values as dv
    JOIN variables as v ON dv.variableId = v.id
    JOIN datasets as d ON v.datasetId = d.id
    WHERE d.version = %(version)s and d.namespace = %(namespace)s and d.shortName = %(dataset)s
    """

    df = pd.read_sql(
        q,
        engine,
        params={"version": version, "namespace": namespace, "dataset": dataset},
    )

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
def dataframes(
    dataframe1: str,
    dataframe2: str,
    absolute_tolerance: float,
    relative_tolerance: float,
    show_values: bool,
    show_shared: bool,
    truncate_lists_at: int,
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
        absolute_tolerance,
        relative_tolerance,
        show_values,
        show_shared,
        truncate_lists_at,
    )
    exit(return_code)


if __name__ == "__main__":
    cli()

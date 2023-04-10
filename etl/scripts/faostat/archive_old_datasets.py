"""Archive unused grapher FAOSTAT datasets and etl steps.

TODO: Most of the logic in this script could (and eventually should) be integrated with VersionTracker.
  Also, for some reason, catalogPath is often missing in DB table variables, which makes this script partially useless.
  We should have a catalog path in the DB datasets table.

"""

import argparse
from typing import List

import pandas as pd
from MySQLdb.connections import Connection
from structlog import get_logger

from etl import db
from etl.helpers import VersionTracker
from etl.scripts.faostat.create_new_steps import find_latest_version_for_step
from etl.scripts.faostat.shared import NAMESPACE

log = get_logger()

INTERACTIVE = True


def add_step_attributes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Extract step names and versions from the catalog path.
    df["namespace"] = df["catalog_path"].str.split("/").str[1]
    df["version"] = df["catalog_path"].str.split("/").str[2]
    df["name"] = df["catalog_path"].str.split("/").str[3]

    return df


def list_active_db_datasets(db_conn: Connection, only_from_etl: bool = False) -> pd.DataFrame:
    query = """
    SELECT d.id, d.name, v.id, v.catalogPath
    FROM datasets d
    JOIN variables v
    ON v.datasetId = d.id
    JOIN chart_dimensions cd
    ON v.id = cd.variableId
    WHERE d.isArchived IS FALSE"""
    if only_from_etl:
        query += """
            AND v.catalogPath IS NOT NULL
        """
    query += ";"
    with db_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    df = pd.DataFrame(result, columns=["dataset_id", "dataset_title", "variable_id", "catalog_path"])

    # Keep only dataset information.
    df = df.drop_duplicates(subset=["dataset_id"]).drop(columns=["variable_id"]).reset_index(drop=True)

    # Add step attributes.
    df = add_step_attributes(df=df)

    return df


def list_archivable_db_datasets(
    db_conn: Connection, tracker: VersionTracker, namespace: str = NAMESPACE
) -> pd.DataFrame:
    # Get all DB datasets (and its variables, with their catalog path) that:
    # * Are not archived.
    # * Were produced by ETL (and therefore have a catalog path).
    # * Do not have any variables in charts (and hence none of their variable ids appear in chart_dimensions).
    # * Belong to a specific namespace.
    query = """
    SELECT d.id, d.name, v.id, v.catalogPath
    FROM datasets d
    JOIN variables v
    ON v.datasetId = d.id
    WHERE d.isArchived IS FALSE
    AND v.catalogPath IS NOT NULL
    AND d.id NOT IN (
        SELECT DISTINCT(v.datasetId)
        FROM chart_dimensions cd
        JOIN variables v
        ON v.id = cd.variableId
        )
    ;
    """
    with db_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    df = pd.DataFrame(result, columns=["dataset_id", "dataset_title", "variable_id", "catalog_path"])

    # Select only datasets for the relevant namespace, and keep only unique dataset ids and catalog paths.
    df = (
        df[df["catalog_path"].str.startswith(f"grapher/{namespace}/")]
        .drop_duplicates(subset=["dataset_id"])
        .drop(columns=["variable_id"])
        .reset_index(drop=True)
    )

    # Add step attributes.
    df = add_step_attributes(df=df)

    # Find the latest ETL version of each step.
    df["latest_version"] = [
        find_latest_version_for_step(channel="grapher", step_name=step_name) for step_name in df["name"]
    ]

    # Select all grapher datasets whose version is not the latest.
    df = (
        df[df["version"] < df["latest_version"]].drop(columns=["catalog_path", "latest_version"]).reset_index(drop=True)
    )

    # Get list of backported datasets ids from the dag.
    backported_dataset_ids = tracker.get_backported_db_dataset_ids()
    not_archivable_datasets = sorted(set(df["dataset_id"]) & set(backported_dataset_ids))

    if len(not_archivable_datasets) > 0:
        log.warning(
            "The following grapher DB datasets cannot be archived because they are backported: "
            f"{not_archivable_datasets}"
        )

    # Remove backported datasets from the list of archivable datasets.
    df = df[~df["dataset_id"].isin(not_archivable_datasets)].reset_index(drop=True)

    # Sort conveniently.
    datasets_to_archive = df.sort_values("dataset_id").reset_index(drop=True)

    return datasets_to_archive


# def list_archived_db_datasets(db_conn: Connection, namespace: str = NAMESPACE) -> pd.DataFrame:
#     # Get all datasets (and its variables, with their catalog path) that:
#     # * Are archived.
#     # * Were produced by ETL (and therefore have a catalog path).
#     query = """
#     SELECT d.id, d.name, v.id, v.catalogPath
#     FROM datasets d
#     JOIN variables v
#     ON v.datasetId = d.id
#     WHERE d.isArchived IS TRUE
#     AND v.catalogPath IS NOT NULL
#     ;
#     """
#     with db_conn.cursor() as cursor:
#         cursor.execute(query)
#         result = cursor.fetchall()

#     df = pd.DataFrame(result, columns=["dataset_id", "dataset_title", "variable_id", "catalog_path"])

#     # Select only datasets for the relevant namespace, and keep only unique dataset ids and catalog paths.
#     df = df[df["catalog_path"].str.startswith(f"grapher/{namespace}/")].drop_duplicates(subset=["dataset_id"]).\
#         drop(columns=["variable_id"]).reset_index(drop=True)

#     # Add step attributes.
#     df = add_step_attributes(df=df)

#     # Sort conveniently.
#     datasets_archived = df.sort_values("dataset_id").reset_index(drop=True)

#     return datasets_archived


def check_db_dataset_is_archivable(dataset_id: int, tracker: VersionTracker, db_conn: Connection) -> None:
    # Check that a DB dataset:
    # * Has no variables used in charts.
    # * Is not backported by active ETL steps.
    db_datasets_active_ids = sorted(set(list_active_db_datasets(db_conn=db_conn)["dataset_id"]))
    error = f"DB dataset with id {dataset_id} cannot be archived because it has variables used in charts."
    assert dataset_id not in db_datasets_active_ids, error

    error = f"DB dataset with id {dataset_id} cannot be archived because it is backported by an active ETL step."
    assert dataset_id not in tracker.get_backported_db_dataset_ids(), error


def archive_db_datasets(
    datasets_to_archive: pd.DataFrame,
    db_conn: Connection,
    tracker: VersionTracker,
    interactive: bool = True,
    execute: bool = False,
) -> None:
    # Double check that each DB dataset can safely be archived.
    [check_db_dataset_is_archivable(dataset_id, tracker=tracker, db_conn=db_conn) for dataset_id in datasets_to_archive]

    if interactive and len(datasets_to_archive) > 0:
        _list = "\n".join(
            [
                f"[{row['version']}/{row['name']}] {row['dataset_id']} - {row['dataset_title']}"
                for _, row in datasets_to_archive.iterrows()
            ]
        )
        log.info(f"\nArchivable DB datasets:\n{_list}")
        if execute:
            input("Press enter to archive all the above DB datasets.")

    # Get ids of datasets to be archived in database.
    dataset_ids_to_archive = datasets_to_archive["dataset_id"].unique().tolist()  # type: ignore

    # Archive (and make private) selected datasets.
    if execute and len(dataset_ids_to_archive) > 0:
        query = f"""
        UPDATE datasets
        SET isPrivate=1, isArchived=1
        WHERE id IN ({','.join([str(i) for i in dataset_ids_to_archive])})
        ;
        """
        with db_conn.cursor() as cursor:
            cursor.execute(query)
        print(f"Archived {len(dataset_ids_to_archive)} datasets.")


def get_etl_paths_for_db_dataset_ids(dataset_ids: List[int], db_conn: Connection) -> pd.DataFrame:
    query = f"""
        SELECT d.id, v.catalogPath
        FROM datasets d
        JOIN variables v
        ON d.id = v.datasetID
        WHERE d.id IN ({','.join([str(i) for i in dataset_ids])})
        ;
    """
    with db_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        catalog_paths = pd.DataFrame(result, columns=["dataset_id", "catalog_path"]).dropna().drop_duplicates()

    ids_with_missing_paths = sorted(set(dataset_ids) - set(catalog_paths["dataset_id"]))
    if len(ids_with_missing_paths) > 0:
        log.error(
            f"Catalog path not found for DB datasets {ids_with_missing_paths}. "
            "Manually check if these ids are found in the name of any backported dataset in the active DAG."
        )

    return catalog_paths


def get_archivable_grapher_steps(db_conn: Connection, tracker: VersionTracker) -> pd.DataFrame:
    # Find all active DB datasets.
    db_datasets_active = list_active_db_datasets(db_conn=db_conn)

    # Find all active ETL grapher steps.
    # Only public steps will be considered. The archival of private steps has be done manually.
    steps_df = tracker.steps_df.copy()
    grapher_steps = steps_df[
        (steps_df["status"] == "active") & (steps_df["channel"] == "grapher") & (steps_df["kind"] == "public")
    ]

    # Warn about grapher steps used as dependencies
    # (this should not happen often, but may happen for fasttracked datasets).
    grapher_steps_used_as_dependencies = sorted(set(grapher_steps["step"]) & set(tracker.all_active_dependencies))
    if len(grapher_steps_used_as_dependencies) > 0:
        _list = "\n".join(grapher_steps_used_as_dependencies)
        log.warning(f"The following grapher steps are used as dependencies of other steps:\n{_list}")

    # Get ETL paths of grapher steps that have a DB dataset that is a backported dependency of an active ETL step.
    backported_db_dataset_ids = tracker.get_backported_db_dataset_ids()
    etl_paths_of_backported_steps = get_etl_paths_for_db_dataset_ids(
        dataset_ids=backported_db_dataset_ids, db_conn=db_conn
    )["catalog_path"].tolist()
    etl_paths_of_backported_steps = [
        "/".join(f"data://{step}".split("/")[:-1]) for step in etl_paths_of_backported_steps if step is not None
    ]

    # Of all ETL grapher steps, find those that do not have any active or backported DB dataset.
    etl_steps_to_archive = pd.merge(
        grapher_steps, db_datasets_active, on=["namespace", "version", "name"], how="outer", indicator=True
    )
    etl_steps_to_archive = etl_steps_to_archive[
        (etl_steps_to_archive["_merge"] == "left_only")
        & ~(etl_steps_to_archive["step"].isin(etl_paths_of_backported_steps))
    ].reset_index(drop=True)

    return etl_steps_to_archive


def main(execute: bool = False) -> None:
    # Initialize connection to DB.
    db_conn = db.get_connection()

    # Initialise version tracker.
    tracker = VersionTracker()

    # List DB datasets that can safely be archived.
    db_datasets_to_archive = list_archivable_db_datasets(tracker=tracker, db_conn=db_conn)

    if len(db_datasets_to_archive) > 0:
        # Archive unused grapher datasets.
        archive_db_datasets(db_datasets_to_archive, db_conn=db_conn, tracker=tracker, execute=execute)

    # Find all ETL grapher steps in the dag that do not have a DB dataset.
    etl_steps_to_archive = get_archivable_grapher_steps(tracker=tracker, db_conn=db_conn)

    # TODO: Eventually it would be good if this happened automatically.
    #  Then, version tracker would also detect other steps to be safely archived.
    if len(etl_steps_to_archive) > 0:
        _list = "\n".join(sorted(etl_steps_to_archive["step"].unique()))
        log.info("\n\nArchivable ETL grapher steps:")
        log.info(
            "\nThe following ETL steps can safely be (manually) moved from the active to the archive dag. "
            f"Make the desired changes and and run this script again:\n{_list}"
        )

    # Apply version tracker sanity checks, to ensure all dependencies of active steps exist,
    # and to warn about steps that can safely be archived.
    tracker.apply_sanity_checks()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-e",
        "--execute",
        default=False,
        action="store_true",
        help="If given, execute archival of DB datasets. Otherwise, simply print the log without writing to DB.",
    )
    args = parser.parse_args()
    main(
        execute=args.execute,
    )

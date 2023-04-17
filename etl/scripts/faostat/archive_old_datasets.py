"""Archive unused grapher FAOSTAT datasets and etl steps.

TODO: Most of the logic in this script could (and eventually should) be integrated with VersionTracker.

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


def list_active_db_datasets(db_conn: Connection) -> pd.DataFrame:
    query = """
    SELECT d.id, d.name, d.namespace, d.version, d.shortName
    FROM datasets d
    JOIN variables v
    ON v.datasetId = d.id
    JOIN chart_dimensions cd
    ON v.id = cd.variableId
    WHERE d.isArchived IS FALSE
    ;
    """
    with db_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    df = pd.DataFrame(result, columns=["dataset_id", "dataset_title", "namespace", "version", "short_name"])

    # Keep only dataset information.
    df = df.drop_duplicates(subset=["dataset_id"]).reset_index(drop=True)

    return df


def list_archivable_db_datasets(
    db_conn: Connection, tracker: VersionTracker, namespace: str = NAMESPACE
) -> pd.DataFrame:
    # Get all DB datasets that:
    # * Are not archived.
    # * Were produced by ETL (and therefore have a version).
    # * Do not have any variables in charts (and hence none of their variable ids appear in chart_dimensions).
    # * Belong to a specific namespace.
    query = """
    SELECT d.id, d.name, d.namespace, d.version, d.shortName
    FROM datasets d
    JOIN variables v
    ON v.datasetId = d.id
    WHERE d.isArchived IS FALSE
    AND d.version IS NOT NULL
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

    df = pd.DataFrame(result, columns=["dataset_id", "dataset_title", "namespace", "version", "short_name"])

    # Select only datasets for the relevant namespace, and keep only unique dataset ids and catalog paths.
    df = df[df["namespace"] == namespace].drop_duplicates(subset=["dataset_id"]).reset_index(drop=True)

    # Find the latest ETL version of each step.
    df["latest_version"] = [
        find_latest_version_for_step(channel="grapher", step_name=step_name) for step_name in df["short_name"]
    ]

    # Select all grapher datasets whose version is not the latest.
    df = df[df["version"] < df["latest_version"]].reset_index(drop=True)

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
                f"[{row['version']}/{row['short_name']}] {row['dataset_id']} - {row['dataset_title']}"
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
        SELECT d.id, d.namespace, d.version, d.shortName
        FROM datasets d
        WHERE d.id IN ({','.join([str(i) for i in dataset_ids])})
        ;
    """
    with db_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=["dataset_id", "namespace", "version", "short_name"])

    ids_with_missing_paths = sorted(set(dataset_ids) - set(df["dataset_id"]))
    if len(ids_with_missing_paths) > 0:
        log.error(
            f"Catalog path not found for DB datasets {ids_with_missing_paths}. "
            "Manually check if these ids are found in the name of any backported dataset in the active DAG."
        )

    # Generate catalog paths, assuming all datasets come from the ETL grapher channel.
    df["catalog_path"] = "grapher/" + df["namespace"] + "/" + df["version"] + "/" + df["short_name"]

    return df


def get_archivable_grapher_steps(db_conn: Connection, tracker: VersionTracker) -> pd.DataFrame:
    # Find all active DB datasets.
    db_datasets_active = list_active_db_datasets(db_conn=db_conn)

    # Find all active ETL grapher steps.
    # Only public steps will be considered. The archival of private steps has be done manually.
    steps_df = tracker.steps_df.copy()
    grapher_steps = steps_df[
        (steps_df["status"] == "active") & (steps_df["channel"] == "grapher") & (steps_df["kind"] == "public")
    ].rename(columns={"name": "short_name"})

    # Warn about grapher steps used as dependencies
    # (this should not happen often, but may happen for fasttracked datasets).
    grapher_steps_used_as_dependencies = grapher_steps[
        grapher_steps["step"].isin(tracker.all_active_dependencies)
    ].reset_index(drop=True)
    if len(grapher_steps_used_as_dependencies) > 0:
        _list = "\n".join(grapher_steps_used_as_dependencies["step"].tolist())
        log.warning(f"The following grapher steps are used as dependencies of other steps:\n{_list}")
    grapher_steps_used_as_dependencies = grapher_steps_used_as_dependencies[["namespace", "version", "short_name"]]

    # Get ETL paths of grapher steps that have a DB dataset that is a backported dependency of an active ETL step.
    backported_db_dataset_ids = tracker.get_backported_db_dataset_ids()
    backported_steps = get_etl_paths_for_db_dataset_ids(dataset_ids=backported_db_dataset_ids, db_conn=db_conn).dropna(
        subset="version"
    )

    # Combine active DB steps, grapher steps used as dependencies of active steps, and backported steps.
    # to gather all not archivable steps.
    not_archivable_steps = (
        pd.concat([db_datasets_active, backported_steps, grapher_steps_used_as_dependencies], ignore_index=True)
        .drop_duplicates(subset=["namespace", "version", "short_name"])
        .reset_index(drop=True)
    )

    # Of all ETL grapher steps, find those that do not have any active or backported DB dataset.
    etl_steps_to_archive = pd.merge(
        grapher_steps, not_archivable_steps, on=["namespace", "version", "short_name"], how="outer", indicator=True
    )
    etl_steps_to_archive = etl_steps_to_archive[etl_steps_to_archive["_merge"] == "left_only"].reset_index(drop=True)

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

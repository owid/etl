"""This module contains functions that interact with ETL, DB, and possibly our API.

Together with utils.db and utils.cached, it might need some rethinking on where it goes.
"""
from pymysql import OperationalError
from sqlalchemy.orm import Session

from apps.wizard.utils.cached import get_datasets_from_version_tracker
from etl.grapher_io import get_all_datasets


def get_steps_df(archived: bool = True):
    """Get steps_df, and grapher_changes from version tracker."""
    # NOTE: The following ignores DB datasets that are archived (which is a bit unexpected).
    # I had to manually un-archive the testing datasets from the database manually to make things work.
    # This could be fixed, but maybe it's not necessary (since we won't archive an old version of a dataset until the
    # new has been analyzed).
    steps_df_grapher, grapher_changes = get_datasets_from_version_tracker()

    # Combine with datasets from database that are not present in ETL
    # Get datasets from Database
    try:
        datasets_db = get_all_datasets(archived=archived)
    except OperationalError as e:
        raise OperationalError(
            f"Could not retrieve datasets. Try reloading the page. If the error persists, please report an issue. Error: {e}"
        )

    # Get table with all datasets (ETL + DB)
    steps_df_grapher = (
        steps_df_grapher.merge(datasets_db, on="id", how="outer", suffixes=("_etl", "_db"))
        .sort_values(by="id", ascending=False)
        .drop(columns="updatedAt")
        .astype({"id": int})
    )
    columns = ["namespace", "name"]
    for col in columns:
        steps_df_grapher[col] = steps_df_grapher[f"{col}_etl"].fillna(steps_df_grapher[f"{col}_db"])
        steps_df_grapher = steps_df_grapher.drop(columns=[f"{col}_etl", f"{col}_db"])

    assert steps_df_grapher["name"].notna().all(), "NaNs found in `name`"
    assert steps_df_grapher["namespace"].notna().all(), "NaNs found in `namespace`"

    return steps_df_grapher, grapher_changes

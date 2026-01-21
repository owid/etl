"""Dataset detection logic for indicator upgrade.

This module provides functions to detect which datasets need indicator upgrades,
based on version tracker changes and database state.
"""

from typing import List, Tuple

import pandas as pd
from pymysql import OperationalError
from rapidfuzz import fuzz
from structlog import get_logger

from apps.wizard.utils.cached import get_datasets_from_version_tracker
from etl.grapher.io import get_all_datasets, get_dataset_charts

log = get_logger()


def detect_dataset_migrations(
    archived: bool = False,
) -> List[Tuple[int, int]]:
    """Detect dataset pairs that need indicator upgrades.

    This function identifies new datasets that have been added to the grapher
    and their corresponding old versions that need indicator migration.

    Args:
        archived: Whether to include archived datasets in the search.

    Returns:
        List of tuples (old_dataset_id, new_dataset_id) representing
        dataset pairs that need indicator upgrades.
    """
    # Get datasets with migration information
    df = get_datasets_with_migrations(archived=archived)

    # Filter to only new datasets that have an old counterpart
    new_datasets = df[df["new_dataset_mappable"]]

    if len(new_datasets) == 0:
        log.warning("No dataset migrations detected")
        return []

    # Extract pairs of (old_id, new_id) from the migration data
    pairs = []
    for _, row in new_datasets.iterrows():
        new_id = row["id"]
        # Find the best matching old dataset by looking at the score columns
        score_cols = [col for col in df.columns if col.startswith(f"score_{new_id}")]
        if score_cols:
            score_col = score_cols[0]
            # Get datasets with score of 200 (these are detected old versions)
            old_candidates = df[df[score_col] == 200]
            if len(old_candidates) > 0:
                # Take the first (most recent) old dataset
                old_id = old_candidates.iloc[0]["id"]
                pairs.append((old_id, new_id))

    log.info(f"Detected {len(pairs)} dataset migration(s)")
    for old_id, new_id in pairs:
        log.info(f"  - Dataset {old_id} -> {new_id}")

    return pairs


def get_datasets_with_migrations(archived: bool = True) -> pd.DataFrame:
    """Get all datasets with migration detection information.

    This function combines data from version tracker and database to identify
    which datasets are new and which old datasets they should replace.

    Args:
        archived: Whether to include archived datasets.

    Returns:
        DataFrame with dataset information and migration flags:
        - migration_new: Boolean indicating if this is a new dataset
        - new_dataset_mappable: Boolean indicating if new dataset has an old counterpart
        - score_{dataset_id}: Similarity scores for matching old datasets
    """
    # Get steps_df and grapher_changes from version tracker
    steps_df_grapher, grapher_changes = get_datasets_from_version_tracker()

    # Combine with datasets from database that are not present in ETL
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

    # Add column marking migrations
    steps_df_grapher["migration_new"] = False
    steps_df_grapher["new_dataset_mappable"] = False
    if grapher_changes:
        dataset_ids = [g["new"]["id"] for g in grapher_changes]
        steps_df_grapher.loc[steps_df_grapher["id"].isin(dataset_ids), "migration_new"] = True

        # Add column ranking possible old datasets
        for g in grapher_changes:
            col_name = f"score_{g['new']['id']}"

            # Create a filter to select the new dataset.
            filter_new = steps_df_grapher["id"] == g["new"]["id"]

            # First, fuzzy match the step short_names (and names to account for old datasets not in ETL)
            score_step = steps_df_grapher["step"].apply(lambda x: fuzz.ratio(g["new"]["step"], x))
            score_name = steps_df_grapher["name"].apply(lambda x: fuzz.ratio(g["new"]["name"], x))
            steps_df_grapher[col_name] = (score_step + score_name) / 2

            # Then, prioritize those detected by grapher_changes ('old' keyword)
            if "old" in g:
                steps_df_grapher.loc[steps_df_grapher["id"] == g["old"]["id"], col_name] = 200
                # This is a new dataset that does have an old counterpart (hence is mappable).
                steps_df_grapher.loc[filter_new, "new_dataset_mappable"] = True

            # Set own dataset as last
            steps_df_grapher.loc[filter_new, col_name] = -1

        # Sort by mappability and number of charts
        # Prefer datasets with old counterparts and fewer charts (likely unmapped)
        new_dataset_charts = get_dataset_charts(dataset_ids=dataset_ids)
        new_dataset_charts["n_new_charts"] = new_dataset_charts["chart_ids"].apply(len)  # type: ignore
        steps_df_grapher = (
            steps_df_grapher.merge(
                new_dataset_charts[["dataset_id", "n_new_charts"]].rename(columns={"dataset_id": "id"}),  # type: ignore
                on="id",
                how="left",
            )
            .sort_values(["new_dataset_mappable", "n_new_charts"], ascending=[False, True], na_position="last")
            .reset_index(drop=True)
        )

    # Replace NaN with empty string in etl paths
    steps_df_grapher["step"] = steps_df_grapher["step"].fillna("")

    return steps_df_grapher

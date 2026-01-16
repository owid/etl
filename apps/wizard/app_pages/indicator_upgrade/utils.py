"""Utils for chart revision tool."""

from typing import Dict, Tuple, cast

import pandas as pd
import streamlit as st
from pymysql import OperationalError
from structlog import get_logger

from apps.indicator_upgrade.detect import get_datasets_with_migrations
from apps.indicator_upgrade.match import find_mapping_suggestions, preliminary_mapping
from etl.db import get_connection
from etl.grapher.io import get_all_datasets, get_variables_in_dataset

# Logger
log = get_logger()


@st.spinner("Retrieving datasets...", show_time=True)
def get_datasets(archived: bool) -> pd.DataFrame:
    """Get datasets with migration detection information.

    This is a wrapper around the detect module's get_datasets_with_migrations function,
    adapted for use in the Streamlit UI.
    """
    # Get datasets with migration information
    steps_df_grapher = get_datasets_with_migrations(archived=archived)

    # Set session state for UI
    st.session_state.is_any_migration = steps_df_grapher["migration_new"].any()

    return steps_df_grapher


def get_datasets_from_db() -> pd.DataFrame:
    """Load datasets."""
    try:
        datasets = get_all_datasets(archived=False)
    except OperationalError as e:
        raise OperationalError(
            f"Could not retrieve datasets. Try reloading the page. If the error persists, please report an issue. Error: {e}"
        )
    else:
        return datasets.sort_values("name")


@st.cache_data(max_entries=1, ttl=60 * 10)
def get_indicators_from_datasets(
    dataset_id_1: int, dataset_id_2: int, show_new_not_in_old: int = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get indicators from two datasets."""
    with get_connection() as db_conn:
        # Get indicators from old dataset that have been used in at least one chart.
        old_indictors = get_variables_in_dataset(db_conn=db_conn, dataset_id=dataset_id_1, only_used_in_charts=True)
        # Get all indicators from new dataset.
        new_indictors = get_variables_in_dataset(db_conn=db_conn, dataset_id=dataset_id_2, only_used_in_charts=False)
        if show_new_not_in_old:
            # Unsure why this was done, but it seems to be wrong.
            # Remove indicators in the new dataset that are not in the old dataset.
            # This can happen if we are matching a dataset to itself in case of renaming variables.
            new_indictors = new_indictors[~new_indictors["id"].isin(old_indictors["id"])]
    return old_indictors, new_indictors


@st.cache_data(show_spinner=False)
def preliminary_mapping_cached(
    old_indicators, new_indicators, match_identical
) -> Tuple[Dict[int, int], pd.DataFrame, pd.DataFrame]:
    """Get preliminary indicator mapping.

    This maps indicators based on names that are identical.
    """
    mapping, missing_old, missing_new = preliminary_mapping(
        old_indicators=old_indicators,
        new_indicators=new_indicators,
        match_identical=match_identical,
    )

    if not mapping.empty:
        indicator_mapping_auto = (
            mapping.astype({"id_old": "int", "id_new": "int"}).set_index("id_old")["id_new"].to_dict()
        )
    else:
        indicator_mapping_auto = {}

    # Cast
    indicator_mapping_auto = cast(Dict[int, int], indicator_mapping_auto)

    return indicator_mapping_auto, missing_old, missing_new


@st.cache_data(show_spinner=False)
def find_mapping_suggestions_cached(missing_old, missing_new, similarity_name):
    """Get mappings for manual mapping.

    Most indicators can't be mapped automatically. This method finds suggestions for each indicator. The user will have to review these and manually choose the best option.
    """
    with st.spinner():
        suggestions = find_mapping_suggestions(
            missing_old=missing_old,
            missing_new=missing_new,
            similarity_name=similarity_name,
        )  # type: ignore
    # Sort by max similarity: First suggestion is that one that has the highest similarity score with any of its suggested new vars.
    suggestions = sorted(suggestions, key=lambda x: x["new"]["similarity"].max(), reverse=True)
    return suggestions

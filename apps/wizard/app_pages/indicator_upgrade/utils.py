"""Utils for chart revision tool."""

from typing import Dict, Tuple, cast

import pandas as pd
import streamlit as st
from pymysql import OperationalError
from rapidfuzz import fuzz
from structlog import get_logger

from apps.wizard.utils.io import get_steps_df
from etl.db import get_connection
from etl.grapher.io import get_all_datasets, get_dataset_charts, get_variables_in_dataset
from etl.match_variables import find_mapping_suggestions, preliminary_mapping

# Logger
log = get_logger()


@st.spinner("Retrieving datasets...")
def get_datasets(archived: bool) -> pd.DataFrame:
    # Get steps_df and grapher_changes
    steps_df_grapher, grapher_changes = get_steps_df(archived=archived)

    # Add column marking migrations
    steps_df_grapher["migration_new"] = False
    steps_df_grapher["new_dataset_mappable"] = False
    if grapher_changes:
        dataset_ids = [g["new"]["id"] for g in grapher_changes]
        steps_df_grapher.loc[steps_df_grapher["id"].isin(dataset_ids), "migration_new"] = True
        # Add column ranking possible old datasets
        ## Criteria:
        for g in grapher_changes:
            col_name = f"score_{g['new']['id']}"

            # Create a filter to select the new dataset.
            filter_new = steps_df_grapher["id"] == g["new"]["id"]
            ##  - First options should be those detected by grapher_changes ('old' keyword)
            if "old" in g:
                steps_df_grapher.loc[steps_df_grapher["id"] == g["old"]["id"], col_name] = 200
                # This is a new dataset that does have an old counterpart (hence is mappable).
                steps_df_grapher.loc[filter_new, "new_dataset_mappable"] = True

            ##  - Then, we should just fuzzy match the step short_names (and names to account for old datasets not in ETL)
            score_step = steps_df_grapher["step"].apply(lambda x: fuzz.ratio(g["new"]["step"], x))
            score_name = steps_df_grapher["name"].apply(lambda x: fuzz.ratio(g["new"]["name"], x))
            steps_df_grapher[col_name] = (score_step + score_name) / 2

            ## Set own dataset as last
            steps_df_grapher.loc[filter_new, col_name] = -1

        if "new_dataset_selectbox" not in st.session_state:
            # If indicator upgrader has been restarted, ensure the new dataset selected has not already been mapped.
            # Add column with the number of charts of new dataset, and then sort steps_df_grapher by that column.
            # This way, the new datasets dropdown will always start with a dataset that has not yet been mapped.
            # NOTE: This could be taken from version tracker, but restarting version tracker takes time, so it's better to get
            # this info directly from db.
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

    st.session_state.is_any_migration = steps_df_grapher["migration_new"].any()

    # Replace NaN with empty string in etl paths (otherwise dataset won't be shown if 'show step names' is chosen)
    steps_df_grapher["step"] = steps_df_grapher["step"].fillna("")

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

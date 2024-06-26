"""Utils for chart revision tool."""
from typing import Any, Dict, List, Tuple, cast

import pandas as pd
import streamlit as st
from pymysql import OperationalError
from rapidfuzz import fuzz
from structlog import get_logger

from apps.utils.map_datasets import get_grapher_changes
from etl.db import config, get_all_datasets, get_connection, get_dataset_charts, get_variables_in_dataset
from etl.git import get_changed_files
from etl.indicator_upgrade.schema import get_schema_chart_config
from etl.match_variables import find_mapping_suggestions, preliminary_mapping
from etl.version_tracker import VersionTracker

# Logger
log = get_logger()


@st.spinner("Retrieving datasets...")
def get_datasets() -> pd.DataFrame:
    steps_df_grapher, grapher_changes = get_datasets_from_version_tracker()

    # Combine with datasets from database that are not present in ETL
    # Get datasets from Database
    try:
        datasets_db = get_all_datasets(archived=False)
    except OperationalError as e:
        raise OperationalError(
            f"Could not retrieve datasets. Try reloading the page. If the error persists, please report an issue. Error: {e}"
        )

    steps_df_grapher = pd.concat([steps_df_grapher, datasets_db], ignore_index=True)
    steps_df_grapher = steps_df_grapher.drop_duplicates(subset="id").drop(columns="updatedAt").astype({"id": int})

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
    return steps_df_grapher


@st.cache_data(show_spinner=False)
def get_datasets_from_version_tracker() -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    # Get steps_df
    vt = VersionTracker()
    assert vt.connect_to_db, "Can't connnect to database! You need to be connected to run indicator upgrader"
    steps_df = vt.steps_df

    # Get file changes -> Infer dataset migrations
    files_changed = get_changed_files()
    grapher_changes = get_grapher_changes(files_changed, steps_df)

    # Only keep grapher steps
    steps_df_grapher = steps_df.loc[
        steps_df["channel"] == "grapher", ["namespace", "identifier", "step", "db_dataset_name", "db_dataset_id"]
    ]
    # Remove unneded text from 'step' (e.g. '*/grapher/'), no need for fuzzymatch!
    steps_df_grapher["step_reduced"] = steps_df_grapher["step"].str.split("grapher/").str[-1]

    ## Keep only those that are in DB (we need them to be in DB, otherwise indicator upgrade won't work since charts wouldn't be able to reference to non-db-existing indicator IDs)
    steps_df_grapher = steps_df_grapher.dropna(subset="db_dataset_id")
    assert steps_df_grapher.isna().sum().sum() == 0
    ## Column rename
    steps_df_grapher = steps_df_grapher.rename(
        columns={
            "db_dataset_name": "name",
            "db_dataset_id": "id",
        }
    )
    return steps_df_grapher, grapher_changes


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


@st.cache_data(show_spinner=False)
def get_schema() -> Dict[str, Any]:
    """Load datasets."""
    with st.spinner("Retrieving schema..."):
        try:
            schema = get_schema_chart_config()
        except OperationalError as e:
            raise OperationalError(
                f"Could not retrieve the schema. Try reloading the page. If the error persists, please report an issue. Error: {e.__traceback__}"
            )
        else:
            return schema


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


def _check_env() -> bool:
    """Check if environment indicators are set correctly."""
    ok = True
    for env_name in ("GRAPHER_USER_ID", "DB_USER", "DB_NAME", "DB_HOST"):
        if getattr(config, env_name) is None:
            ok = False
            st.warning(st.markdown(f"Environment variable `{env_name}` not found, do you have it in your `.env` file?"))

    if ok:
        st.success("`.env` configured correctly")
    return ok


def _show_environment() -> None:
    """Show environment indicators (streamlit)."""
    # show indicators (from .env)
    st.info(
        f"""
    * **GRAPHER_USER_ID**: `{config.GRAPHER_USER_ID}`
    * **DB_USER**: `{config.DB_USER}`
    * **DB_NAME**: `{config.DB_NAME}`
    * **DB_HOST**: `{config.DB_HOST}`
    """
    )


@st.cache_resource
def _check_env_and_environment() -> None:
    """Check if environment indicators are set correctly."""
    ok = _check_env()
    if ok:
        # check that you can connect to DB
        try:
            with st.spinner():
                _ = get_connection()
        except OperationalError as e:
            st.error(
                "We could not connect to the database. If connecting to a remote database, remember to"
                f" ssh-tunel into it using the appropriate ports and then try again.\n\nError:\n{e}"
            )
            ok = False
        except Exception as e:
            raise e
        else:
            msg = "Connection to the Grapher database was successfull!"
            st.success(msg)
            st.subheader("Environment")
            _show_environment()


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

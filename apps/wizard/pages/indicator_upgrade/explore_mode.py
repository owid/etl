"""Tools for 'explore mode'.

This is currently shown in the indicator upgrader, but might be moved to chart-diff in the future.
"""
from typing import Dict, Tuple, cast

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from structlog import get_logger

log = get_logger()

# Error columns (only if numeric)
COLUMN_RELATIVE_ERROR = "Relative error [(x - y) / x, %]"
COLUMN_ABS_RELATIVE_ERROR = "(abs) Relative error [abs(x - y) / x, %]"
COLUMN_LOG_ERROR = "Log error [log(x - y)]"  # (this one only if all values are positive)
COLUMN_ABS_LOG_ERROR = "(abs) Log error [abs(log(x - y))]"  # (this one only if all values are positive)


def st_explore_indicator(df, indicator_old, indicator_new, var_id_to_display) -> None:
    """Compare `indicator_old` and `indicator_new`.

    * df: raw data
    * indicator_old: variableId of the old indicator
    * indicator_new: variableId of the new indicator
    * var_id_to_display: mapping of variableId to display name. Includes all variables in the dataset. See step 1.1 in indicator_mapping script.

    Show:

        - Similarity score
        - Number of rows changed
        - Table with rows changed (and relative errors)
        - Distribution of change:
            - If indicator is numeric: distribution of relative error
            - If indicator is string/categorical: distribution of categories in old and new indicator
    """
    log.info("table: comparison of two indicators")

    # 1/ Get comparison table
    df_indicators, is_numeric = get_comparison_table(df, indicator_old, indicator_new)

    # 2/ Get similarity score
    score = get_similarity_score(df_indicators, indicator_old, indicator_new)

    # 3/ Show score
    col1, col2, col3 = st.columns([1, 1, 4])
    st_show_score(score, col1, col2)

    # 4 other info (% of rows changed, number of rows changed)
    st_show_details(df_indicators, indicator_old, indicator_new, col3, is_numeric)

    # Rename, remove equal datapoints
    df_indicators = df_indicators.loc[df_indicators[(indicator_old)] != df_indicators[indicator_new]]
    df_indicators = df_indicators.rename(columns=var_id_to_display)
    st.write(df_indicators)

    # 3/ Show number of rows changed

    # 4/ Show table with rows changed

    # 5/ Show distribution of change

    # score = round(100 - df_indicators["Relative difference (abs, %)"].mean(), 1)
    # if score == 100:
    #     score = round(100 - df_indicators["Relative difference (abs, %)"].mean(), 2)
    #     if score == 100:
    #         score = round(100 - df_indicators["Relative difference (abs, %)"].mean(), 3)
    #         if score == 100:
    #             score = round(100 - df_indicators["Relative difference (abs, %)"].mean(), 4)
    # num_nan_score = df_indicators["Relative difference (abs, %)"].isna().sum()

    # nrows_0 = df_indicators.shape[0]
    # ## Keep only rows with relative difference != 0
    # df_indicators = df_indicators[df_indicators["Relative difference (abs, %)"] != 0]
    # ## Keep only rows with different values (old != new)
    # df_indicators = df_indicators[
    #     df_indicators[var_id_to_display[indicator_old]] != df_indicators[var_id_to_display[indicator_new]]
    # ]
    # nrows_1 = df_indicators.shape[0]

    # # Row sanity check
    # ## (Streamlit has a limit on the number of rows it can show)
    # cell_limit = 262144
    # num_cells = df_indicators.shape[0] * df_indicators.shape[1]
    # if num_cells > cell_limit:
    #     num_rows_new = cell_limit // df_indicators.shape[1]
    #     df_indicators = df_indicators.head(num_rows_new)
    #     st.warning(f"Cell limit reached. Only showing first {num_rows_new} rows.")

    # # Show preliminary information
    # nrows_change_relative = round(100 * nrows_1 / nrows_0, 1)
    # col1, col2 = st.columns([1, 5])
    # with col1:
    #     st.metric(
    #         "Data matching score (%)",
    #         score,
    #         help="The data matching score is based on the average of the relative difference between the two indicators. A high score indicates a good match. It is estimated as `100 - average(relative scores)`.",
    #     )
    # with col2:
    #     st.info(
    #         f"""
    #         - {num_nan_score} rows with unknown score
    #         - {nrows_change_relative} % of the rows changed ({nrows_1} out of {nrows_0})
    #     """
    #     )
    # # Show table
    # st.dataframe(df_indicators)

    # # Show distribution of relative change
    # fig = px.histogram(
    #     df_indicators, x="Relative difference (abs, %)", nbins=100, title="Distribution of relative change"
    # )
    # st.plotly_chart(fig, use_container_width=True)


def correct_dtype(series: pd.Series) -> pd.Series:
    """Convert series to an appropriate type (float or category)."""
    try:
        series = series.astype(float)
    except ValueError:
        try:
            series = series.astype("category")
        except ValueError:
            raise ValueError("Could not convert to category (or float previously)")
    return series


# @st.cache_data(show_spinner=False)
def get_comparison_table(
    df: pd.DataFrame,
    indicator_old: str,
    indicator_new: str,
) -> Tuple[pd.DataFrame, bool]:
    """Create comparison df.

    Columns:
        - entityName
        - year
        - indicator_old
        - indicator_new

        Additional columns if indicator is numeric:
            - Absolute difference
            - Relative difference (abs, %)
    """
    # 0/ Filter to keep only relevant indicators (old and new)
    df_variables = df.loc[df["variableId"].isin([indicator_old, indicator_new])].copy()

    # 1/ Find out if indicator is numeric or not
    df_variables["value"] = correct_dtype(df_variables["value"])
    is_numeric = pd.api.types.is_numeric_dtype(df_variables["value"])

    # 2/ Reshape dataframe
    df_variables = df_variables.pivot(index=["entityName", "year"], columns="variableId", values="value").reset_index()

    # 3/ Add error columns if numeric
    if is_numeric:
        df_variables = _add_error_columns(df_variables, indicator_old, indicator_new)

    return df_variables, is_numeric


def _add_error_columns(df: pd.DataFrame, indicator_old: str, indicator_new: str) -> pd.DataFrame:
    """Add error columns to the dataframe.

    If the indicator is numeric, add columns:
        - Absolute difference
        - Relative difference (abs, %)
    """
    # 1/ Add relative error
    ## Estimate error only when indicator_old is not 0. If indicator_old is 0, the relative error is infinite.
    mask = df[indicator_old] == 0
    df.loc[~mask, COLUMN_RELATIVE_ERROR] = (
        (100 * (df.loc[~mask, indicator_old] - df.loc[~mask, indicator_new]) / df.loc[~mask, indicator_old]).round(2)
    ).round(2)
    df.loc[mask, COLUMN_RELATIVE_ERROR] = float("inf")

    # Add absolute
    df[COLUMN_ABS_RELATIVE_ERROR] = df[COLUMN_RELATIVE_ERROR].abs()

    # df = df.sort_values(COLUMN_RELATIVE_ERROR, ascending=False)

    # 2/ Add log error (only if there are no negative values)
    if (df[indicator_old] >= 0).all() and (df[indicator_new] >= 0).all():
        mask_old_0 = df[indicator_old] == 0
        mask_new_0 = df[indicator_new] == 0
        mask = ~(mask_old_0 | mask_new_0)
        df.loc[mask, COLUMN_LOG_ERROR] = (
            np.log10(df.loc[mask, indicator_old]) - np.log10(df.loc[mask, indicator_new])
        ).round(2)
        df.loc[mask_old_0, COLUMN_LOG_ERROR] = float("inf")
        df.loc[mask_new_0, COLUMN_LOG_ERROR] = -float("inf")

        # Add absolute
        df[COLUMN_ABS_LOG_ERROR] = df[COLUMN_LOG_ERROR].abs()
    return df


def get_similarity_score(
    df: pd.DataFrame, column_old: str | None = None, column_new: str | None = None
) -> Dict[str, float]:
    """Get similarity score between old and new indicators.

    - numeric only positive: mean log error, and mean relative error.
    - numeric with negatives: mean relative error.
    - categorical/string: % of datapoints that are different.
    """
    score = {}
    N_ROUND_DEC = 2
    if COLUMN_ABS_LOG_ERROR in df.columns:
        # Only positive values
        with pd.option_context("mode.use_inf_as_na", True):
            score["log_error"] = df.loc[:, COLUMN_ABS_LOG_ERROR].dropna().mean().round(N_ROUND_DEC)

    if COLUMN_ABS_RELATIVE_ERROR in df.columns:
        # Numeric values
        with pd.option_context("mode.use_inf_as_na", True):
            score["rel_error"] = df.loc[:, COLUMN_ABS_RELATIVE_ERROR].dropna().mean().round(N_ROUND_DEC)
    if (COLUMN_LOG_ERROR not in df.columns) and (COLUMN_RELATIVE_ERROR not in df.columns):
        # Categorical values
        assert (column_old is not None) and (
            column_new is not None
        ), "Need to provide column names for categorical values."
        score["rel_diff_error"] = (100 * ((df.loc[:, column_old] != df.loc[:, column_new]).astype(int).mean())).round(2)
        # num_diff = (df.loc[:, column_old] != df.loc[:, column_new]).sum()
        # score["num_diff"] = num_diff
        # score["num_totla"] = len(df)

    return score


def st_show_score(score, col1, col2):
    """Show similarity scores.

    col1: streamlit column for main score
    col2: streamlit column for secondary score
    """

    def st_show_error_relative(score):
        st.metric(
            "Average relative error",
            f"{score['rel_error']} %",
            help="The average relative error is the average of the relative difference between the two indicators. A high score indicates a bad match.",
        )

    def st_show_error_log(score):
        st.metric(
            "Average log error",
            f"{score['log_error']} dB",
            help="The average log error is the average of the log error. The log error is defined as the difference between the logs of the old and new indicators: `log(old) - log(new)`. A value close to zero means very few errors. A value of 2, means that on average, there is a 2-order magnitude of error (e.g. 10 times more or less than expected).",
        )

    def st_show_error_diff(score):
        st.metric(
            "Share of different datapoints",
            f"{score['rel_diff_error']} %",
            help="This indicates the percentage of all datapoints (both and new) that are distinct between old and new indicators.",
        )

    # Find out the case: (numeric positive => 1 column, numeric w negatives => 2 columns, categorical => 1 column)
    if ("log_error" in score) and ("rel_error" in score):
        with col1:
            st_show_error_relative(score)
        with col2:
            st_show_error_log(score)
    elif "rel_error" in score:
        with col1:
            st_show_error_relative(score)
    elif "rel_diff_error" in score:
        with col1:
            st_show_error_diff(score)


def st_show_details(df, indicator_old, indicator_new, col, is_numeric):
    with col:
        text = []
        # Number of unknown scores
        if is_numeric:
            num_nan_score = df[COLUMN_RELATIVE_ERROR].isna().sum()
            text.append(f"**{num_nan_score}** rows with unknown score")
        # Number of rows changed
        nrows_0 = df.shape[0]
        nrows_1 = (df[(indicator_old)] != df[indicator_new]).sum()
        nrows_change_relative = round(100 * nrows_1 / nrows_0, 1)
        text.append(f"**{nrows_change_relative} %** of the rows changed ({nrows_1} out of {nrows_0})")
        # Number of NAs is new indicator
        num_nan_new = df[indicator_new].isna().sum()
        text.append(f"**{num_nan_new}** NAs in new indicator")

        st.info("- " + "\n- ".join(text))

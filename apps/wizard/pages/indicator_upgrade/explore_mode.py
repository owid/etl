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
# INDEX COLUMNS
COLUMNS_INDEX = ["entityName", "year"]


@st.experimental_dialog("Explore changes in the new indicator", width="large")
def st_explore_indicator_dialog(df, indicator_old, indicator_new, var_id_to_display) -> None:
    """Same as st_explore_indicator but framed in a dialog.

    More on dialogs: https://docs.streamlit.io/develop/api-reference/execution-flow/st.dialog
    """
    st_explore_indicator(df, indicator_old, indicator_new, var_id_to_display)


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
    if not all(col in df.columns for col in COLUMNS_INDEX):
        st.warning(f"This indicator is missing index columns {COLUMNS_INDEX}. Can't run explore mode.")
        return None

    log.info("table: comparison of two indicators")

    # 1/ Get comparison table
    df_indicators, is_numeric = get_comparison_table(df, indicator_old, indicator_new)

    # 2/ Get similarity score
    score = get_similarity_score(df_indicators, indicator_old, indicator_new)

    # TODO: maybe structure the information in tabs?
    # tab1, tab2, tab3 = st.tabs(["Summary", "Changes in datapoints", "Error distribution"])

    # 3/ Show score
    st_show_score(score)

    # 4/ other info (% of rows changed, number of rows changed)
    st_show_details(df_indicators, indicator_old, indicator_new, is_numeric)

    # Rename, remove equal datapoints
    df_indicators = df_indicators.loc[df_indicators[(indicator_old)] != df_indicators[indicator_new]]
    df_indicators = df_indicators.rename(columns=var_id_to_display)

    # 5/ Show dataframe with different rows
    st.header("Changes in data points")
    st_show_dataframe(df_indicators)

    # 6/ Show distribution of change
    st_show_plot(df_indicators, col_old=var_id_to_display[indicator_old], col_new=var_id_to_display[indicator_new])


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

        # Re-order
        df = df[
            COLUMNS_INDEX
            + [
                indicator_old,
                indicator_new,
                COLUMN_RELATIVE_ERROR,
                COLUMN_LOG_ERROR,
                COLUMN_ABS_RELATIVE_ERROR,
                COLUMN_ABS_LOG_ERROR,
            ]
        ]
    else:
        # Re-order
        df = df[
            COLUMNS_INDEX
            + [
                indicator_old,
                indicator_new,
                COLUMN_RELATIVE_ERROR,
                COLUMN_ABS_RELATIVE_ERROR,
            ]
        ]

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


def st_show_score(score):
    """Show similarity scores.

    col1: streamlit column for main score
    col2: streamlit column for secondary score
    """

    def st_show_error_relative(score):
        st.metric(
            "Average relative error",
            f"{round(score['rel_error'], 2)}%",
            help="The average relative error is the average of the relative difference between the two indicators. A high score indicates a bad match.",
        )

    def st_show_error_log(score):
        st.metric(
            "Average log error",
            f"{round(score['log_error'], 2)}dB",
            help="The average log error is the average of the log error. The log error is defined as the difference between the logs of the old and new indicators: `log(old) - log(new)`. A value close to zero means very few errors. A value of 2, means that on average, there is a 2-order magnitude of error (e.g. 10 times more or less than expected).",
        )

    def st_show_error_diff(score):
        st.metric(
            "Share of different datapoints",
            f"{round(score['rel_diff_error'], 2)}%",
            help="This indicates the percentage of all datapoints (both and new) that are distinct between old and new indicators.",
        )

    # Find out the case: (numeric positive => 1 column, numeric w negatives => 2 columns, categorical => 1 column)
    if ("log_error" in score) and ("rel_error" in score):
        col1, col2 = st.columns(2)
        with col1:
            st_show_error_relative(score)
        with col2:
            st_show_error_log(score)
    elif "rel_error" in score:
        # with col1:
        st_show_error_relative(score)
    elif "rel_diff_error" in score:
        # with col1:
        st_show_error_diff(score)


def st_show_details(df, indicator_old, indicator_new, is_numeric):
    # with col:
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

    st.info("## Sumary\n- " + "\n- ".join(text))


def st_show_dataframe(df: pd.DataFrame) -> None:
    """Show dataframe accounting for cell limit and good sorting."""
    df_show = df.copy()

    # Limit number of rows
    cell_limit = 262144
    num_cells = df_show.shape[0] * df.shape[1]
    if num_cells > cell_limit:
        num_rows_new = cell_limit // df.shape[1]
        df_show = df_show.head(num_rows_new)
        st.warning(f"Cell limit reached. Only showing first {num_rows_new} rows.")

    # Sort by error
    if COLUMN_ABS_RELATIVE_ERROR in df_show.columns:
        df_show = df_show.sort_values(COLUMN_ABS_RELATIVE_ERROR, ascending=False)  # type: ignore

    # Show
    st.dataframe(df_show)


def st_show_plot(df: pd.DataFrame, col_old: str, col_new: str) -> None:
    if not ((COLUMN_RELATIVE_ERROR in df.columns) or (COLUMN_LOG_ERROR in df.columns)):
        # Reshape
        st.write(df[col_old].unique())
        df_cat = df.melt(id_vars=COLUMNS_INDEX, value_vars=[col_old, col_new], var_name="indicator", value_name="value")
        counts = df_cat.groupby(["indicator", "value"]).size().reset_index(name="count")
        pivot_df = counts.pivot(index="value", columns="indicator", values="count").fillna(0).reset_index()
        # st.dataframe(pivot_df)
        pivot_df["avg"] = pivot_df[[col for col in pivot_df.columns if col != "value"]].mean(axis=1)
        # pivot_df["avg"] = (df[col_old].astype(float).fillna(0) + df[col_new].astype(float).fillna(0)) / 2
        pivot_df = pivot_df.sort_values("avg", ascending=False).drop(columns="avg")
        pivot_df.columns = ["value"] + [f"count_{col}" for col in pivot_df.columns if col != "value"]
        # st.write(df_cat)
        # x = df_cat.groupby(["indicator"])["value"].value_counts()
        st.write(pivot_df)
        # df_cat["value"] = df_cat["value"].astype("string").fillna("NaN")
        # categories = list(set(df_cat["value"]))
        # categories_map = {cat: i for i, cat in enumerate(categories)}
        # df_cat["value"] = df_cat["value"].map(categories_map)

        # st.dataframe(df_cat.value.unique())
        # fig = px.histogram(
        #     df_cat,
        #     x="value",
        #     color="indicator",
        #     # barmode="overlay",
        #     barmode="group",
        #     # nbins=100,
        #     title="Distribution of relative error",
        #     text_auto=True,
        #     color_discrete_map={
        #         col_old: "blue",
        #         col_new: "red",
        #     },
        #     category_orders={"value": categories},
        #     opacity=0.7,
        # )

        # # Update layout to modify the x-axis labels
        # custom_labels = {k: v for k, v in categories_map.items()}
        # st.write(custom_labels)
        # st.plotly_chart(fig, use_container_width=True)
        # # Apply custom tick labels to x-axis
        # fig.update_layout(
        #     xaxis=dict(tickmode="array", tickvals=list(custom_labels.keys()), ticktext=list(custom_labels.values()))
        # )

        # st.plotly_chart(fig, use_container_width=True)
    else:
        if COLUMN_RELATIVE_ERROR in df.columns:
            fig = px.histogram(df, x=COLUMN_RELATIVE_ERROR, nbins=100, title="Distribution of relative error")
            st.plotly_chart(fig, use_container_width=True)
        if COLUMN_LOG_ERROR in df.columns:
            fig = px.histogram(df, x=COLUMN_LOG_ERROR, nbins=100, title="Distribution of relative log error")
            st.plotly_chart(fig, use_container_width=True)

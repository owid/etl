"""Concerns the second stage of wizard charts, when the variable mapping is constructed."""
import uuid
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
from pydantic import BaseModel
from structlog import get_logger

from apps.backport.datasync.data_metadata import variable_data_df_from_s3
from apps.wizard.pages.charts.utils import get_variables_from_datasets
from apps.wizard.utils.env import OWIDEnv
from etl.db import get_engine
from etl.match_variables import find_mapping_suggestions, preliminary_mapping

# Logger
log = get_logger()

# Ignore all variables (boxes are checked for all variables, hence no variable will be considered in the variable mapping)
IGNORE_ALL = False


def ask_and_get_variable_mapping(search_form, owid_env: OWIDEnv) -> "VariableConfig":
    """Ask and get variable mapping."""
    variable_config = VariableConfig()

    ###########################################################################
    # 1/ PROCESSING: Get variables, find similarities and suggestions, etc.
    ###########################################################################

    # 1.1/ INTERNAL PROCESSING
    # Get variables from old and new datasets
    old_variables, new_variables = get_variables_from_datasets(
        search_form.dataset_old_id,
        search_form.dataset_new_id,
        show_new_not_in_old=False,
    )

    # 1.2/ Build display mappings: id -> display_name
    ## This is to display the variables in the selectboxes with format "[id] name"
    df = pd.concat([old_variables, new_variables], ignore_index=True)  # .drop_duplicates()
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    variable_id_to_display = df.set_index("id")["display_name"].to_dict()

    # 1.3/ Get auto variable mapping (if mapping by identical name is enabled)
    ## Note that when the old and new datasets are the same, this option is disabled. Otherwise all variables are mapped (which probably does not make sense?)
    if search_form.dataset_old_id == search_form.dataset_new_id:
        match_identical_vars = False
    else:
        match_identical_vars = search_form.map_identical_variables
    mapping, missing_old, missing_new = preliminary_mapping(
        old_variables,
        new_variables,
        match_identical=match_identical_vars,
    )
    if not mapping.empty:
        variable_mapping_auto = mapping.set_index("id_old")["id_new"].to_dict()
    else:
        variable_mapping_auto = {}

    # 1.4/ Get remaining mapping suggestions
    # This is for those variables which couldn't be automatically mapped
    suggestions = find_mapping_suggestions(missing_old, missing_new, search_form.similarity_function_name)  # type: ignore
    # Sort by max similarity: First suggestion is that one that has the highest similarity score with any of its suggested new vars.
    suggestions = sorted(suggestions, key=lambda x: x["new"]["similarity"].max(), reverse=True)

    # [OPTIONAL] EXPLORE MODE
    # Get data points
    if search_form.enable_explore_mode:
        with st.spinner(
            "Retrieving data values from S3. This might take some time... If you don't need this, disable the 'Explore' option from the 'parameters' section."
        ):
            df_data = get_variable_data_cached(list(set(old_variables["id"]) | set(new_variables["id"])))

    ###########################################################################
    # 2/ DISPLAY: Show the variable mapping form
    ###########################################################################
    # 2.1/ DISPLAY MAPPING SECTION
    ## Header
    st.header(
        "Map variables",
        help="Map variables from the old to the new dataset. The idea is that the variables in the new dataset will replace those from the old dataset in our charts. You can choose to ignore some variables if you want to.",
    )
    if not variable_mapping_auto and not suggestions:
        st.warning(
            f"It looks as the dataset [{search_form.dataset_old_id}](https://owid.cloud/admin/datasets/{search_form.dataset_old_id}) has no variable in use in any chart! Therefore, no mapping is needed."
        )
    else:
        with st.container():
            col1, col2 = st.columns(2)
            col_1_widths = [6, 1]
            col_2_widths = [7, 1, 1] if search_form.enable_explore_mode else [6, 1]

            # Titles
            with col1:
                st.subheader("Old dataset")
                col11, col12 = st.columns(col_1_widths)
                with col11:
                    st.caption(f"[Explore dataset]({owid_env.admin_site}/datasets/{search_form.dataset_old_id}/)")
                with col12:
                    st.caption("Ignore", help="Check to ignore this variable in the mapping.")
                    IGNORE_ALL = st.checkbox(
                        "All",
                        help="Check to ignore all mappings.",
                    )
            with col2:
                st.subheader("New dataset")
                cols2 = st.columns(col_2_widths)
                with cols2[0]:
                    st.caption(f"[Explore dataset]({owid_env.admin_site}/datasets/{search_form.dataset_new_id}/)")
                with cols2[1]:
                    st.caption(
                        "Score",
                        help="Similarity score between the old variable and the 'closest' new variable (from 0 to 100%). Variables with low scores are likely not to have a good match.",
                    )
                if search_form.enable_explore_mode:
                    with cols2[2]:
                        st.caption(
                            "Explore",
                            help="Explore the distribution of the currently compared variables.",
                        )
            old_var_selectbox = []
            ignore_selectbox = []
            new_var_selectbox = []

            # Automatically mapped variables (non-editable)
            for i, (variable_old, variable_new) in enumerate(variable_mapping_auto.items()):
                with st.container():
                    col_auto_1, col_auto_2 = st.columns(2)
                    with col_auto_1:
                        cols_auto = st.columns(col_1_widths)
                        with cols_auto[0]:
                            element = st.selectbox(
                                label=f"auto-{i}-left",
                                options=[variable_old],
                                disabled=True,
                                label_visibility="collapsed",
                                format_func=variable_id_to_display.get,
                            )
                            old_var_selectbox.append(element)
                        with cols_auto[1]:
                            element = st.checkbox(
                                "Ignore", key=f"auto-ignore-{i}", label_visibility="collapsed", value=IGNORE_ALL
                            )
                            ignore_selectbox.append(element)
                    with col_auto_2:
                        cols_auto_2 = st.columns(col_2_widths)
                        with cols_auto_2[0]:
                            element = st.selectbox(
                                label=f"auto-{i}-right",
                                options=[variable_new],
                                disabled=True,
                                label_visibility="collapsed",
                                format_func=variable_id_to_display.get,
                            )
                            new_var_selectbox.append(element)
                        with cols_auto_2[1]:
                            st.markdown(":violet[**100%**]")
                        if search_form.enable_explore_mode:
                            with cols_auto_2[2]:
                                element_check = st.checkbox(
                                    "Explore", key=f"auto-explore-{i}", label_visibility="collapsed"
                                )
                    if search_form.enable_explore_mode and element_check:  # type: ignore
                        try:
                            plot_comparison_two_variables(df_data, variable_old, variable_new, variable_id_to_display)  # type: ignore
                        except Exception:
                            st.error(
                                "Something went wrong! This can be due to several reasons: One (or both) of the variables are not numeric, `values` for one of the variables does not have the columns `entityName` and `year`. Please check the error message below. Report the error #002001"
                            )
            # Remaining variables (editable)
            for i, suggestion in enumerate(suggestions):
                with st.container():
                    variable_old = suggestion["old"]["id_old"]
                    similarity_max = int(suggestion["new"]["similarity"].max().round(0))
                    col_manual_1, col_manual_2 = st.columns(2)
                    with col_manual_1:
                        col_manual_11, col_manual_12 = st.columns(col_1_widths)
                        with col_manual_11:
                            element = st.selectbox(
                                label=f"manual-{i}-left",
                                options=[variable_old],
                                disabled=True,
                                label_visibility="collapsed",
                                format_func=variable_id_to_display.get,
                            )
                            old_var_selectbox.append(element)
                        with col_manual_12:
                            element = st.checkbox(
                                "Ignore", key=f"manual-ignore-{i}", label_visibility="collapsed", value=IGNORE_ALL
                            )
                            ignore_selectbox.append(element)
                    with col_manual_2:
                        cols_manual_2 = st.columns(col_2_widths)
                        with cols_manual_2[0]:
                            element = st.selectbox(
                                label=f"manual-{i}-right",
                                options=suggestion["new"]["id_new"],
                                disabled=False,
                                label_visibility="collapsed",
                                format_func=variable_id_to_display.get,
                            )
                            new_var_selectbox.append(element)
                        with cols_manual_2[1]:
                            if similarity_max > 80:
                                color = "blue"
                            elif similarity_max > 60:
                                color = "green"
                            elif similarity_max > 40:
                                color = "orange"
                            else:
                                color = "red"
                            st.markdown(f":{color}[**{similarity_max}%**]")
                        if search_form.enable_explore_mode:
                            with cols_manual_2[2]:
                                element_check = st.checkbox(
                                    "Explore", key=f"manual-explore-{i}", label_visibility="collapsed"
                                )
                    if search_form.enable_explore_mode and element_check:  # type: ignore
                        plot_comparison_two_variables(df_data, variable_old, element, variable_id_to_display)  # type: ignore

            # Submission button
            submitted_variables = st.button("Next", type="primary")
            if submitted_variables or st.session_state.show_submission_details:
                st.session_state.submitted_variables = True
                st.session_state.show_submission_details = True
                st.session_state.submitted_revisions = False
                log.info(
                    f"{st.session_state.submitted_datasets}, {st.session_state.submitted_variables}, {st.session_state.submitted_revisions}"
                )
                # BUILD MAPPING
                variable_mapping = _build_variable_mapping(old_var_selectbox, new_var_selectbox, ignore_selectbox)
                variable_config = VariableConfig(variable_mapping=variable_mapping)
            else:
                st.session_state.submitted_variables = False
    return variable_config


class VariableConfig(BaseModel):
    is_valid: bool = False
    variable_mapping: Dict[int, int] = {}

    def __init__(self, **data: Any) -> None:
        """Construct variable config object."""
        if "variable_mapping" in data:
            data["is_valid"] = True
        super().__init__(**data)


def _build_variable_mapping(old_var_selectbox, new_var_selectbox, ignore_selectbox) -> Dict[int, int]:
    if len(old_var_selectbox) != len(new_var_selectbox):
        raise ValueError("Something went wrong! The number of old and new variables is different.")
    if len(old_var_selectbox) != len(ignore_selectbox):
        raise ValueError("Something went wrong! The number of old variables and ignore checkboxes is different.")
    if len(new_var_selectbox) != len(ignore_selectbox):
        raise ValueError("Something went wrong! The number of new variables and ignore checkboxes is different.")
    variable_mapping = {
        int(old): int(new)
        for old, new, ignore in zip(old_var_selectbox, new_var_selectbox, ignore_selectbox)
        if not ignore
    }
    return variable_mapping


@st.cache_data(show_spinner=False)
def get_variable_data_cached(variables_ids: List[int]):
    df = variable_data_df_from_s3(get_engine(), variable_ids=[int(v) for v in variables_ids], workers=10)
    return df


@st.cache_data(show_spinner=False)
def build_df_comparison_two_variables_cached(df, variable_old, variable_new, var_id_to_display):
    # Get df with only the two variables, cast to appropriate type
    df_variables = df[df["variableId"].isin([variable_old, variable_new])]
    df_variables.loc[:, "value"] = df_variables.value.astype(float)
    # Reshape dataframe
    df_variables = df_variables.pivot(index=["entityName", "year"], columns="variableId", values="value").reset_index()
    df_variables["Relative difference (abs, %)"] = (
        (100 * (df_variables[variable_old] - df_variables[variable_new]) / df_variables[variable_old]).round(2)
    ).abs()
    df_variables = df_variables.rename(columns=var_id_to_display).sort_values(
        "Relative difference (abs, %)", ascending=False
    )
    # df_variables = df_variables.style.bar(subset=['Relative difference (%)'], color='#d65f5f')
    return df_variables


# @st.cache_data(show_spinner=False)
def plot_comparison_two_variables(df, variable_old, variable_new, var_id_to_display) -> None:
    log.info("table: comparison of two variables")
    df_variables = build_df_comparison_two_variables_cached(df, variable_old, variable_new, var_id_to_display)
    # Show country filters
    countries = st.multiselect(
        "Select locations",
        sorted(set(df_variables["entityName"])),
        key=f"multi-{variable_old}-{variable_new}-{uuid.uuid4().hex[:10]}",
    )
    # st.write(countries)
    if countries:
        df_variables = df_variables[df_variables["entityName"].isin(countries)]
    # Display table
    st.dataframe(
        df_variables.style.background_gradient(cmap="OrRd", subset=["Relative difference (abs, %)"], vmin=0, vmax=20)
    )

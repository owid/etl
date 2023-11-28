"""Concerns the second stage of wizard charts, when the variable mapping is constructed."""
import uuid
from typing import Any, Dict, List, Literal

import pandas as pd
import streamlit as st
from pydantic import BaseModel
from structlog import get_logger

from apps.backport.datasync.data_metadata import variable_data_df_from_s3
from apps.wizard.charts.utils import OWIDEnv, get_variables_from_datasets
from etl.db import get_engine
from etl.match_variables import find_mapping_suggestions, preliminary_mapping

# Logger
log = get_logger()

RADIO_OPTIONS = {
    0: "No",
    1: "Only if it is a small dataset update (Recommended)",
    2: "Yes",
}
ignore_all = False


def ask_and_get_variable_mapping(search_form, owid_env: OWIDEnv) -> "VariableConfig":
    """Ask and get variable mapping."""
    variable_config = VariableConfig()

    # 2.1 INTERNAL PROCESSING
    # Get variables from old and new datasets
    old_variables, new_variables = get_variables_from_datasets(search_form.dataset_old_id, search_form.dataset_new_id)

    # Build display mappings: id -> display_name
    df = pd.concat([old_variables, new_variables], ignore_index=True)
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    variable_id_to_display = df.set_index("id")["display_name"].to_dict()

    # Get auto variable mapping (if mapping by identical name is enabled)
    mapping, missing_old, missing_new = preliminary_mapping(
        old_variables, new_variables, search_form.map_identical_variables
    )
    if not mapping.empty:
        variable_mapping_auto = mapping.set_index("id_old")["id_new"].to_dict()
    else:
        variable_mapping_auto = {}
    # Get remaining mapping suggestions
    suggestions = find_mapping_suggestions(missing_old, missing_new, search_form.similarity_function_name)  # type: ignore
    # Sort by max similarity: First suggestion is that one that has the highest similarity score with any of its suggested new vars
    suggestions = sorted(suggestions, key=lambda x: x["new"]["similarity"].max(), reverse=True)

    # Get data points
    if search_form.enable_explore_mode:
        with st.spinner(
            "Retrieving data values from S3. This might take some time... If you don't need this, disable the 'Explore' option from the 'parameters' section."
        ):
            df_data = get_variable_data_cached(list(set(old_variables["id"]) | set(new_variables["id"])))

    # with st.expander("👷  Mapping details (debugging)"):
    #     st.subheader("Variable mapping")
    #     st.markdown("##### Automatically mapped variables")
    #     st.write(variable_mapping_auto)
    #     st.markdown("##### Variables that need manual mapping from the user.")
    #     for suggestion in suggestions:
    #         st.markdown(f"##### Variable #{suggestion['old']['id_old']}")
    #         st.write(suggestion["old"])
    #         st.write(suggestion["new"].head(5).to_dict())

    # 2.2 DISPLAY MAPPING SECTION
    st.header(
        "Map variables",
        help="Map variables from the old to the new dataset. The idea is that the variables in the new dataset will replace those from the old dataset in our charts. You can choose to ignore some variables if you want to.",
    )
    if not variable_mapping_auto and not suggestions:
        st.warning(
            f"It looks as the dataset [{search_form.dataset_old_id}](https://owid.cloud) has no variable in use in any chart! Therefore, no mapping is needed."
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
                    st.caption(f"[Explore dataset]({owid_env.admin_url}/datasets/{search_form.dataset_old_id}/)")
                with col12:
                    st.caption("Ignore", help="Check to ignore this variable in the mapping.")
                    ignore_all = st.checkbox(
                        "All",
                        help="Check to ignore all mappings.",
                    )
            with col2:
                st.subheader("New dataset")
                cols2 = st.columns(col_2_widths)
                with cols2[0]:
                    st.caption(f"[Explore dataset]({owid_env.admin_url}/datasets/{search_form.dataset_new_id}/)")
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
                                "Ignore", key=f"auto-ignore-{i}", label_visibility="collapsed", value=ignore_all
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
                                "Ignore", key=f"manual-ignore-{i}", label_visibility="collapsed", value=ignore_all
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

            # Options
            skip_slider_check = st.radio(
                label="Review the time slider in charts",
                options=RADIO_OPTIONS.keys(),
                index=1,
                help=(
                    "Review that the new selected timeline range is consistent with the new timeline range.\n\n"
                    "To do this, we need to get the data values for *all* the variables involved (i.e. not only the variables"
                    "being updated). This is a costly operation, and hence we recommend skipping whenever there are more than 50 variables involved.\n\n"
                    "Note, also, that the time slider should almost never be updated. If set to a specific year (or range of years) we should"
                    "assume that there is a good editorial reason for that. If set to 'earliest' or 'latest', the chart will be rendered with the new time range, so no need"
                    "to update the time slider config field value."
                ),
                format_func=RADIO_OPTIONS.get,
            )
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
                variable_config = VariableConfig(variable_mapping=variable_mapping, skip_slider_check=skip_slider_check)
            else:
                st.session_state.submitted_variables = False
    return variable_config


class VariableConfig(BaseModel):
    is_valid: bool = False
    variable_mapping: Dict[int, int] = {}
    skip_slider_check: Literal[0, 1, 2] = 0

    def __init__(self, **data: Any) -> None:
        """Constructor."""
        print(data)
        if "variable_mapping" in data and "skip_slider_check" in data:
            data["is_valid"] = True
        super().__init__(**data)

    @property
    def skip_slider_check_limit(self):
        match self.skip_slider_check:
            # No review
            case 0:
                return -1
            # Only review for small dataset updates (<50 variables involved)
            case 1:
                return 50
            # Review all
            case 2:
                return 1e6
            # No case? default to option 1
            case _:
                return 50


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
def plot_comparison_two_variables(df, variable_old, variable_new, var_id_to_display):
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
    # years = sorted(set(df_variables["year"]))
    # year = st.select_slider('Year', years)
    # df_variables_year = df_variables[df_variables["year"] == year]
    # chart = alt.Chart(df_variables_year).mark_bar().encode(
    #     x="diff",
    #     y="entityName",
    #     tooltip='entityName',
    # ).interactive()
    # st.altair_chart(chart, theme="streamlit", use_container_width=True)

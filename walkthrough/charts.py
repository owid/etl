"""This is the step to create variable mappings and submit chart revisions to Grapher.

1. User chooses which new dataset should replace old dataset (along with other parameters).
2. User is asked to map each variable in old dataset to a variable in new dataset.
3. If user is satisfied with the mapping, the chart revisions are created and submission details are shown to the user.
4. If they confirm, then revisions are submitted to Grapher DB.
"""
from pathlib import Path
from typing import Any, Callable, List, Union

import pandas as pd
from pywebio import input as pi
from pywebio import output as po

import etl
from etl.chart_revision_suggester import ChartRevisionSuggester
from etl.db import (
    get_all_datasets_names,
    get_connection,
    get_dataset_id,
    get_variables_in_dataset,
)
from etl.match_variables_from_two_versions_of_a_dataset import (
    SIMILARITY_NAME,
    SIMILARITY_NAMES,
    find_mapping_suggestions,
    preliminary_mapping,
)

from .utils import OWIDEnv, _check_env, _show_environment

CURRENT_DIR = Path(__file__).parent

ETL_DIR = Path(etl.__file__).parent.parent


def app(run_checks: bool, dummy_data: bool) -> None:

    nav = Navigation(run_checks)

    # show instructions
    nav.show_instructions()
    # show and check environment
    nav.check_environment()
    # get main parameters (old and new dataset names, etc.)
    nav.get_input_params()
    # show params
    nav.show_input_params()
    # get mapping suggestions
    nav.get_suggestions()

    if nav.suggestions:
        # run interactive part of the app
        po.put_markdown(
            "## Map variables\nFor each old variable, map it to a new variable from the drop down menu. Note that you"
            " can select option 'ignore' too."
        )
        nav.run_app()
    else:
        po.put_error(
            "No variable mapping suggestions found. Please check your input parameters. This is probably due to no"
            " variable in the old dataset being used in any chart."
        )


class Navigation:
    # parameters to run commands (given by user)
    params: dict = dict()
    # List of suggested variable mappings
    suggestions: list = list()
    # mapping from old to new variables IDs (after user interaction)
    variable_mapping: dict = dict()
    # OWID Env
    owid_env = OWIDEnv()

    def __init__(self, run_checks) -> None:
        self.run_checks = run_checks

    def init_app(self):
        # show instructions
        self.show_instructions()
        # show and check environment
        self.check_environment()
        # get main parameters (old and new dataset names, etc.)
        self.get_input_params()
        # show params
        self.show_input_params()
        # get mapping suggestions
        self.get_suggestions()

    def show_instructions(self) -> None:
        """Show initial step description."""
        with open(CURRENT_DIR / "charts.md", "r") as f:
            po.put_markdown(f.read())

    def check_environment(self) -> None:
        """Check environment.

        Check if .env is there, show environment variables.
        """
        if self.run_checks:
            _check_env()
        _show_environment()

    def get_input_params(self) -> None:
        """Ask user for input parameters.

        Collects necessary parameters from the user, including old and new dataset names, similarity function, and whether to pair identical variables.
        """
        # ask user for dataset names
        datasets_available = get_all_datasets_names()
        params = pi.input_group(
            "Options",
            [
                pi.select(
                    "Old dataset name",
                    name="old_dataset",
                    value="Life expectancy - Riley (2005), Clio Infra (2015), and UN (2019)",
                    options=datasets_available,
                    required=True,
                ),
                pi.select(
                    "New dataset name",
                    name="new_dataset",
                    value="Life Expectancy (various sources)",
                    options=datasets_available,
                    required=True,
                ),
                pi.select(
                    "Similarity matching function",
                    name="similarity_function",
                    options=[
                        {"label": f_name, "value": f_name, "selected": True}
                        if f_name == SIMILARITY_NAME
                        else {"label": f_name, "value": f_name, "selected": False}
                        for f_name in SIMILARITY_NAMES
                    ],
                    help_text="Select the prefered function for matching variables",
                ),
                pi.checkbox(
                    "",
                    options=["Pair identical variables"],
                    value=["Pair identical variables"],
                    name="pair_identical",
                    help_text="Assume that identically named variables in both old and new datasets should be paired.",
                ),
            ],
        )
        self.params = params
        print(params)

    def show_input_params(self) -> None:
        """Shows user the input parameters."""
        po.put_info(
            po.put_markdown(
                f"""
            * **Old dataset**: *{self.params['old_dataset']}*
            * **New dataset**: *{self.params['new_dataset']}*
            * **Similarity matching function**: {self.params['similarity_function']}
            * Pairing identically named variables is **{'enabled' if self.params['pair_identical'] else 'disabled'}**
            """
            )
        )

    def get_suggestions(self) -> None:
        """Get suggestions for each variable in old dataset.

        Creates a list with new variable suggestions for each old variable. Each item is a dictionary with two keys:

        - "old": Dictionary with old variable name and ID.
        - "new": pandas.DataFrame with new variable names, IDs, sorted by similarity to old variable name (according to matching_function).
        """
        old_dataset_name = self.params["old_dataset"]
        new_dataset_name = self.params["new_dataset"]
        similarity_name = self.params["similarity_function"]
        omit_identical = self.params["pair_identical"]

        with get_connection() as db_conn:
            # Get old and new dataset ids.
            old_dataset_id = get_dataset_id(db_conn=db_conn, dataset_name=old_dataset_name)
            new_dataset_id = get_dataset_id(db_conn=db_conn, dataset_name=new_dataset_name)

            # Get variables from old dataset that have been used in at least one chart.
            old_variables = get_variables_in_dataset(
                db_conn=db_conn, dataset_id=old_dataset_id, only_used_in_charts=True
            )
            # Get all variables from new dataset.
            new_variables = get_variables_in_dataset(
                db_conn=db_conn, dataset_id=new_dataset_id, only_used_in_charts=False
            )

        # get initial mapping
        _, missing_old, missing_new = preliminary_mapping(old_variables, new_variables, omit_identical)
        # get suggestions for mapping
        self.suggestions = find_mapping_suggestions(missing_old, missing_new, similarity_name)

    def run_app(self) -> None:
        """Run remaining steps:

        1. Ask user for variable mapping.
        2. Confirm mapping via popup. If confirmed, got to next step. If not, go back to step 1.
        3. Submit variable mapping to database.
        """
        # checks
        self._sanity_checks()

        # show user variable form, collect their input
        self.get_variable_mapping_ids()

        self.confirm_variable_mapping(next_step=self.final_steps)

    def _sanity_checks(self) -> None:
        """Sanity checks on class attributes `params` and `suggestions`.

        These should have been assigned a value before calling this function. This function checks
        that the value assigned makes sense in terms of typing and key-values.
        """
        assert isinstance(self.suggestions, list), "Suggestions must be a list!"
        for i, s in enumerate(self.suggestions):
            assert all(k in s for k in ("new", "old")), f"Keys 'new' and 'old' missing in suggestin number {i}!."
            assert isinstance(
                s["new"], pd.DataFrame
            ), f"Suggestion number {i} has a 'new' value that is not a DataFrame. Instead {type(s['new'])} was found."
            assert isinstance(
                s["old"], dict
            ), f"Suggestion number {i} has a 'old' value that is not a Series. Instead {type(s['old'])} was found."
        assert isinstance(self.params, dict), "Params must be a dictionary!"
        assert all(
            k in self.params for k in ("old_dataset", "new_dataset", "similarity_function", "pair_identical")
        ), "Check all expected keys are in params!"

    def get_variable_mapping_ids(self) -> None:
        """Show user a form to select new variable names for old variables."""
        selects = []
        for suggestion in self.suggestions:
            old_varname = suggestion["old"]["name_old"]
            old_id = suggestion["old"]["id_old"]
            variables_new = suggestion["new"]

            # create options
            options = [{"label": "(Ignore)", "value": -1}] + [
                {"label": f"{v['name_new']} ({v['similarity']} %)", "value": v["id_new"]}
                for _, v in variables_new.iterrows()
            ]

            default_value = self.variable_mapping.get(old_id, variables_new.iloc[0]["id_new"])
            select = pi.select(
                f"Old variable: {old_varname}",
                name=str(old_id),
                options=options,
                value=default_value,
                required=True,
                # action=("ignore", _ignore_mapping)
                # name=str(old_id),
            )
            selects.append(select)

        # display input group
        mapping = pi.input_group("Variable mapping", selects)

        # clean and sanity checks on mapping
        mapping = {int(k): int(v) for k, v in mapping.items()}

        self.variable_mapping = mapping

    def confirm_variable_mapping(self, next_step: Callable) -> None:
        """Confirm variable mapping by means of a popup."""

        # close popup and go to next step
        def _next_step():
            po.close_popup()
            next_step()

        # close popup and repeat step run_app
        def _repeat_step():
            po.close_popup()
            self.run_app()

        with po.popup("This is your current variable mapping", closable=False, implicit_close=False, size="large"):
            self.show_variable_mapping_table()
            if len(set(self.variable_mapping.values())) != len(self.variable_mapping):
                po.put_warning("Warning: Multiple old variables are mapped to the same new variable!")
            po.put_buttons(
                [
                    {"label": "Confirm", "value": 1, "color": "primary"},
                    {"label": "Edit", "value": 0, "color": "secondary"},
                ],
                onclick=[_next_step, _repeat_step],
            )

    def final_steps(self) -> None:
        """Run final steps once user is satisfied with variable mapping:

        - show mapping table.
        - submit suggestions to Grapher DB.
        - show next steps.
        """
        # show variable mapping
        self.show_variable_mapping_table()
        # clean variable mapping (ignore -1)
        self.variable_mapping = {k: v for k, v in self.variable_mapping.items() if v != -1}
        # build suggester
        suggester = ChartRevisionSuggester(self.variable_mapping)
        # show charts to be updated (and therefore get revisions)
        revisions = self.show_submission_details(suggester)
        # check revisions is not emptu/None
        if revisions:
            # ask user to confirm submission
            action = pi.actions("Confirm submission", [{"label": "Confirm", "value": 1, "color": "success"}])
            if action == 1:
                # submit suggestions to DB
                exit_code = self.submit_suggestions(suggester, revisions)
                # show next steps
                if exit_code == 0:
                    approval_chart_link = self.owid_env.chart_approval_tool_url
                    po.put_markdown(
                        f"""
                    ## Next steps

                    Go to the [Chart approval tool]({approval_chart_link}) and approve, flag or reject the suggested chart revisions.
                    """
                    )

    def show_variable_mapping_table(self):
        """Show variable mapping table.

        Build table with variable mapping (old -> new names). Cell items in table are clickable,
        directing the user to the variable grapher UI page.
        """
        variable_id_to_names = self._varid_to_varname()
        tdata = [
            [
                po.put_link(
                    f"{variable_id_to_names.get(old_id)} ({old_id})",
                    self.owid_env.variable_admin_link(old_id),
                    new_window=True,
                ),
                po.put_link(
                    f"{variable_id_to_names.get(new_id)} ({new_id})",
                    self.owid_env.variable_admin_link(new_id),
                    new_window=True,
                )
                if new_id != -1
                else variable_id_to_names.get(new_id),
            ]
            for old_id, new_id in self.variable_mapping.items()
        ]
        po.put_table(tdata, header=["Old variable", "New variable"])

    def _varid_to_varname(self) -> pd.DataFrame:
        """Build mapping from variable ID to variable NAME."""
        mappings = [pd.DataFrame({"id": [-1], "name": ["(Ignore)"]})]
        for suggestion in self.suggestions:
            old_id = suggestion["old"]["id_old"]
            old_varname = suggestion["old"]["name_old"]
            mapping_old = pd.DataFrame({"id": [old_id], "name": [old_varname]})
            mapping_new = suggestion["new"][["id_new", "name_new"]].rename(columns={"id_new": "id", "name_new": "name"})
            mappings.append(
                pd.concat(
                    [
                        mapping_old,
                        mapping_new,
                    ],
                    ignore_index=True,
                )
            )
        return pd.concat(mappings, ignore_index=True).drop_duplicates().set_index("id").squeeze().to_dict()

    def show_submission_details(self, suggester: ChartRevisionSuggester) -> Union[List[dict[str, Any]], None]:
        """Show submission details.

        This includes the variable id mapping, but also the charts that will be affected by the mapping.
        """
        # get ID mapping without ignore ones (-1)
        po.put_markdown("## Submission details")
        if not self.variable_mapping:
            po.put_error("Mapping is empty!")
        else:
            po.put_markdown("### Variable ID mapping to be submitted")
            po.put_code(self.variable_mapping, "json")
            po.put_markdown("### Charts affected")
            try:
                suggested_chart_revisions = suggester.prepare()
            except Exception as e:
                po.put_error(f"Error: {e}")
                return
            else:
                po.put_markdown(
                    f"There are **{len(suggested_chart_revisions)} charts** that will be affected by the mapping."
                )
            return suggested_chart_revisions

    def submit_suggestions(
        self, suggester: ChartRevisionSuggester, suggested_chart_revisions: List[dict[str, Any]]
    ) -> int:
        """Submit suggestinos to Grapher.

        If successfull, a green box with success message is shown. Otherwise, red box with error message is shown.
        """
        po.put_markdown("## Submission to Grapher")
        po.put_markdown("### Grapher response")

        with po.put_loading():
            po.put_text("Submitting to Grapher...")
            try:
                suggester.insert(suggested_chart_revisions=suggested_chart_revisions)
            except Exception as e:
                po.put_error(e)
                approval_chart_link = self.owid_env.chart_approval_tool_url
                po.put_markdown(
                    f"You might want to go to the [Chart approval tool]({approval_chart_link}) and review! Then, re-run"
                    " `walkthrough charts` again."
                )
                return -1
            else:
                po.put_success("Chart revisions have been submitted to Grapher!")
        return 0

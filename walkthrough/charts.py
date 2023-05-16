"""This is the step to create variable mappings and submit chart revisions to Grapher.

1. User chooses which new dataset should replace old dataset (along with other parameters).
2. User is asked to map each variable in old dataset to a variable in new dataset.
3. If user is satisfied with the mapping, the chart revisions are created and submission details are shown to the user.
4. If they confirm, then revisions are submitted to Grapher DB.
"""
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union, cast

import pandas as pd
import pywebio
import structlog
from MySQLdb import OperationalError
from pywebio import input as pi
from pywebio import output as po

from etl.chart_revision.v1.deprecated import ChartRevisionSuggester
from etl.chart_revision.v1.revision import (
    ChartVariableUpdateRevision,
    get_charts_to_update,
    submit_revisions_to_grapher,
)
from etl.db import get_all_datasets, get_connection, get_variables_in_dataset
from etl.match_variables import (
    SIMILARITY_NAME,
    SIMILARITY_NAMES,
    find_mapping_suggestions,
    preliminary_mapping,
)

from .utils import OWIDEnv, _check_env, _show_environment

# Paths
CURRENT_DIR = Path(__file__).parent
# logs
log = structlog.get_logger()
# OWID Env
OWID_ENV = OWIDEnv()
# set style
with open(CURRENT_DIR / "charts.styles.css", "r") as f:
    pywebio.config(css_style=f.read())


def app(run_checks: bool) -> None:
    nav = Navigation()
    # show live banner alert
    nav.show_live_banner()
    # show instructions
    nav.show_instructions()
    # show and check environment
    if run_checks:
        ok = nav.check_environment()
    else:
        ok = True

    # if checks were succesfull, proceed
    if ok:
        po.put_markdown("## Dataset update")
        # get main parameters (old and new dataset names, etc.)
        nav.get_input_params()

        if nav.params_is_valid():
            # show params
            nav.show_input_params()
            # get mapping suggestions
            nav.get_suggestions_and_mapping()

            if nav.anything_to_map:
                po.toast("Dataset details submitted succesfully!", color="success")
                po.put_markdown("## Map variables")
                # no automated mapping, just suggestions (need user input)
                if not nav.automatically_mapped and nav.has_suggestions:
                    po.put_markdown(
                        "For each old variable, map it to a new variable from the drop down menu. Note that you can"
                        " select option 'ignore' too."
                    )
                # some variables were mapped automaticall, some were not
                elif nav.automatically_mapped and nav.has_suggestions:
                    po.put_markdown(
                        "Some variables were automatically mapped. Please check them and change if necessary."
                    )
                    po.put_markdown(
                        "For the rest of the variables, map each old variable to a new variable from the selection"
                        " menus. Note that you can select option 'ignore' too."
                    )
                # all variables were mapped automatically
                elif nav.automatically_mapped and not nav.has_suggestions:
                    po.put_markdown("All variables were automatically mapped! No need for user input.")
                nav.run_app()
            else:
                po.put_error(
                    "No variable mapping suggestions found. Please check your input parameters. This is probably due to"
                    " no variable in the old dataset being used in any chart."
                )


class Navigation:
    # parameters to run commands (given by user)
    params: dict = dict()
    # List of suggested variable mappings
    suggestions: list = list()
    # mapping from old to new variables IDs (after user interaction)
    variable_mapping_auto: dict = dict()
    variable_mapping_manual: dict = dict()
    # IDs to names
    # __variable_id_to_names: Optional[Dict[int, str]] = None
    __variable_id_to_all: Optional[Dict[int, Dict[str, Any]]] = None

    @property
    def variable_mapping(self) -> Dict[int, int]:
        """Get variable mapping."""
        return self.variable_mapping_auto | self.variable_mapping_manual

    @property
    def automatically_mapped(self):
        """Check if any variable was automatically mapped."""
        # True if any variable was automatically mapped
        return bool(self.variable_mapping_auto)

    @property
    def anything_to_map(self) -> bool:
        """Check if there is anything to map.

        There is something to map if there are either suggestions (user manual input required) or a preliminary mapping.
        """
        return (len(self.suggestions) > 0) or self.variable_mapping != {}

    @property
    def has_suggestions(self) -> bool:
        """Check if there are suggestions."""
        return len(self.suggestions) > 0

    @property
    def variable_id_to_all(self) -> Dict[int, Dict[str, Any]]:
        """Get variable ID to name mapping."""
        # get id to all fields
        if self.__variable_id_to_all is None:
            self.__variable_id_to_all = self._varid_to_all()
        return self.__variable_id_to_all

    @property
    def variable_id_to_name(self) -> Dict[int, str]:
        """Get variable ID to name mapping."""
        # get id to name
        mapping = {i: k["name"] for i, k in self.variable_id_to_all.items()}
        return mapping

    def show_live_banner(self) -> None:
        if OWIDEnv().env_type_id == "live":
            po.put_warning(
                po.put_markdown(
                    "You are trying to connect to the **live** database! Working with live should only be done after"
                    " prototyping and testing on staging. If you are not sure what this means, please"
                    " consult the with ETL team."
                )
            )

    def show_instructions(self) -> None:
        """Show initial step description."""
        log.info("1. Showing instructions")
        po.put_markdown("# Walkthrough - Charts")
        po.put_info(po.put_markdown("`charts` step can be only executed by OWID staff!"))
        with open(CURRENT_DIR / "charts.md", "r") as f:
            po.put_markdown(f.read())

    def check_environment(self) -> bool:
        """Check environment.

        Check if .env is there, show environment variables.
        """
        log.info("2. Checking environment")
        po.put_markdown("""## Environment""")
        # Check that .env is there
        po.put_markdown("""### Checking...""")
        ok = _check_env()
        if ok:
            # check that you can connect to DB
            try:
                _ = get_connection()
            except OperationalError as e:
                po.put_error(
                    "We could not connect to the database. If connecting to a remote database, remember to"
                    f" ssh-tunel into it using the appropriate ports and then try again.\n\nError:\n{e}"
                )
                ok = False
            except Exception as e:
                raise e
            else:
                msg = "Connection to the Grapher database was successfull!"
                po.put_success(msg)
                po.toast(msg, color="success")
                ok = True
                # show variables (from .env)
                po.put_markdown("""### Variables""")
                _show_environment()
        return ok

    def get_input_params(self) -> None:
        """Ask user for input parameters.

        Collects necessary parameters from the user, including old and new dataset names, similarity function, and whether to pair identical variables.
        """
        log.info("3. Getting input parameters")
        # ask user for dataset names
        datasets_available = get_all_datasets(archived=False)
        names_available = sorted(set(datasets_available["name"]))

        def get_available_ids_for_name(name: str):
            ids = cast(pd.Series, datasets_available[datasets_available["name"] == name]["id"])
            ids = sorted(ids.tolist())
            if len(ids) == 1:
                return [{"label": i, "value": i, "disabled": True, "selected": True} for i in ids]
            return [i for i in ids]

        # default selected datasets
        name_old_default = names_available[10]
        ids_old_default = get_available_ids_for_name(name_old_default)
        name_new_default = names_available[11]
        ids_new_default = get_available_ids_for_name(name_new_default)

        params = pi.input_group(
            "Options",
            [
                pi.select(
                    "Old dataset name",
                    name="dataset_old_name",
                    options=names_available,
                    value=name_old_default,
                    onchange=lambda n: pi.input_update(
                        "dataset_old_id",
                        options=get_available_ids_for_name(n),
                    ),
                    required=True,
                ),
                pi.select(
                    "Old dataset id",
                    name="dataset_old_id",
                    options=ids_old_default,
                    help_text=(
                        "Sometimes there can be multiple datasets with the same name. In that case, you need to specify"
                        " the dataset id."
                    ),
                    required=True,
                ),
                pi.select(
                    "New dataset name",
                    name="dataset_new_name",
                    value=name_new_default,
                    options=names_available,
                    onchange=lambda n: pi.input_update(
                        "dataset_new_id",
                        options=get_available_ids_for_name(n),
                    ),
                    required=True,
                ),
                pi.select(
                    "New dataset id",
                    name="dataset_new_id",
                    options=ids_new_default,
                    help_text=(
                        "Sometimes there can be multiple datasets with the same name. In that case, you need to specify"
                        " the dataset id."
                    ),
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
                    help_text=(
                        "Select the prefered function for matching variables. Find more details at"
                        " https://www.analyticsvidhya.com/blog/2021/07/fuzzy-string-matching-a-hands-on-guide/"
                    ),
                ),  #
                pi.checkbox(
                    "",
                    options=["Pair identical variables"],
                    value=["Pair identical variables"],
                    name="match_identical",
                    help_text=(
                        "Assume that identically named variables in both old and new datasets should be paired. Uncheck"
                        " if you want to manually map them."
                    ),
                ),
                pi.checkbox(
                    "",
                    options=["Use old version"],
                    value=[],
                    name="old_version",
                    help_text="Currently using latest backend version. Check to use old version!",
                ),
            ],
        )

        # check if ids are none, which means that there is only one dataset
        if params["dataset_old_id"] is None:
            params["dataset_old_id"] = get_available_ids_for_name(params["dataset_old_name"])[0]["value"]
        if params["dataset_new_id"] is None:
            params["dataset_new_id"] = get_available_ids_for_name(params["dataset_new_name"])[0]["value"]

        self.params = params

    def params_is_valid(self):
        """Check if parameters are valid.

        Parameters corresponds to the configuration given at the beginning by the user.
        """
        if self.params["dataset_old_id"] == self.params["dataset_new_id"]:
            msg = "Old and new datasets cannot be the same!"
            po.toast(msg, color="error")
            po.put_error(msg)
            return False
        return True

    def show_input_params(self) -> None:
        """Shows user the input parameters."""
        log.info("4. Showing input parameters")
        link_old = OWID_ENV.dataset_admin_url(self.params["dataset_old_id"])
        # dataset_admin_url
        link_new = OWID_ENV.dataset_admin_url(self.params["dataset_new_id"])
        po.put_info(
            po.put_html(
                f"""
            <ul>
                <li><b><a href='{link_old}'>Old dataset ↗</a></b>
                    <ul>
                        <li><b>Name</b>: {self.params['dataset_old_name']}</li>
                        <li><b>ID</b>: {self.params['dataset_old_id']}</li>
                    </ul>
                </li>
                <li><b><a href='{link_new}'>New dataset ↗</a></b>
                    <ul>
                        <li><b>Name</b>: {self.params['dataset_new_name']}</li>
                        <li><b>ID</b>: {self.params['dataset_new_id']}</li>
                    </ul>
                </li>
                <li><b>Similarity matching function</b>: {self.params['similarity_function']}</li>
                <li>Pairing identically named variables is <b>{'enabled' if self.params['match_identical'] else 'disabled'}</b></li>
            </ul>
            """
            )
        )

    def get_suggestions_and_mapping(self) -> None:
        """Get suggestions for each variable in old dataset.

        Creates a list with new variable suggestions for each old variable. Each item is a dictionary with two keys:

        - "old": Dictionary with old variable name and ID.
        - "new": pandas.DataFrame with new variable names, IDs, sorted by similarity to old variable name (according to matching_function).
        """
        log.info("5. Getting variable mapping suggestions")
        similarity_name = self.params["similarity_function"]
        match_identical = self.params["match_identical"]

        with get_connection() as db_conn:
            # Get variables from old dataset that have been used in at least one chart.
            old_variables = get_variables_in_dataset(
                db_conn=db_conn, dataset_id=self.params["dataset_old_id"], only_used_in_charts=True
            )
            # Get all variables from new dataset.
            new_variables = get_variables_in_dataset(
                db_conn=db_conn, dataset_id=self.params["dataset_new_id"], only_used_in_charts=False
            )

        # get initial mapping
        mapping, missing_old, missing_new = preliminary_mapping(old_variables, new_variables, match_identical)
        if not mapping.empty:
            self.variable_mapping_auto = mapping.set_index("id_old")["id_new"].to_dict()
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
        if self.has_suggestions:
            self.get_variable_mapping_ids()
            self.confirm_variable_mapping(next_step=self.final_steps)
        else:
            log.info("No suggestions available. Skipping input form and confirmation step.")
            self.final_steps()

    def _sanity_checks(self) -> None:
        """Sanity checks on class attributes `params` and `suggestions`.

        These should have been assigned a value before calling this function. This function checks
        that the value assigned makes sense in terms of typing and key-values.
        """
        log.info("6. Sanity checks")
        assert isinstance(self.suggestions, list), "Suggestions must be a list!"
        if (len(self.suggestions) == 0) and not self.variable_mapping:
            log.info("No value was assigned to neither `self.suggestions` nor `self.variable_mapping`.")
        if len(self.suggestions) > 0:
            for i, s in enumerate(self.suggestions):
                assert all(k in s for k in ("new", "old")), f"Keys 'new' and 'old' missing in suggestin number {i}!."
                assert isinstance(s["new"], pd.DataFrame), (
                    f"Suggestion number {i} has a 'new' value that is not a DataFrame. Instead {type(s['new'])} was"
                    " found."
                )
                assert isinstance(
                    s["old"], dict
                ), f"Suggestion number {i} has an 'old' value that is not a Series. Instead {type(s['old'])} was found."
        assert isinstance(self.params, dict), "Params must be a dictionary!"
        assert all(
            k in self.params
            for k in (
                "dataset_old_name",
                "dataset_new_name",
                "dataset_old_id",
                "dataset_new_id",
                "similarity_function",
                "match_identical",
            )
        ), "Check all expected keys are in params!"

    def get_variable_mapping_ids(self) -> None:
        """Show user a form to select new variable names for old variables."""
        log.info("7. Getting variable mapping ids")
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

        self.variable_mapping_manual = mapping
        log.info(f"Variable mapping (manual): {mapping}")

    def confirm_variable_mapping(self, next_step: Callable) -> None:
        """Confirm variable mapping by means of a popup."""

        log.info("8. Confirming variable mapping with user with popup")

        # close popup and go to next step
        def _next_step():
            po.close_popup()
            next_step()

        # close popup and repeat step run_app
        def _repeat_step():
            po.close_popup()
            self.run_app()

        with po.popup("This is your current variable mapping", closable=False, implicit_close=False, size="large"):
            self.show_variable_mapping_table(separate=True)
            var_map = {old: new for old, new in self.variable_mapping.items() if new != -1}
            if len(set(var_map.values())) != len(var_map):
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
        log.info("9. Final steps")
        # show variable mapping
        self.show_variable_mapping_table(separate=True)
        # clean variable mapping (ignore -1)
        self._clean_variable_mapping()
        # TODO: this is a hack to allow for legacy version
        if self.params["old_version"]:
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
                        po.put_markdown(
                            f"""
                        ## Next steps

                        Go to the [Chart approval tool]({OWID_ENV.chart_approval_tool_url}) and approve, flag or reject the suggested chart revisions.
                        """
                        )
        else:
            # Get revisions to be made
            log.info("Get charts to update...")
            charts = get_charts_to_update(self.variable_mapping)
            # show charts to be updated (and therefore get revisions)
            log.info("Showing submission details...")
            revisions = self.show_submission_details_exp(charts)
            if revisions:
                # ask user to confirm submission
                action = pi.actions("Confirm submission", [{"label": "Confirm", "value": 1, "color": "success"}])
                if action == 1:
                    # submit suggestions to DB
                    exit_code = self.submit_suggestions_exp(revisions)
                    # show next steps
                    if exit_code == 0:
                        po.put_markdown(
                            f"""
                        ## Next steps

                        Go to the [Chart approval tool]({OWID_ENV.chart_approval_tool_url}) and approve, flag or reject the suggested chart revisions.
                        """
                        )

    def show_variable_mapping_table(self, separate: bool = False):
        """Show variable mapping table.

        Build table with variable mapping (old -> new names). Cell items in table are clickable,
        directing the user to the variable grapher UI page.

        Parameters
        ----------
        separate : bool, optional
            If True, table is shown separated by those mappings that were created manually vs. automatically.
        """

        def _show_table(mapping: Dict[int, int]):
            table = """
            <div style="overflow: auto;">
                <table style>
                    <tbody>
                        <tr>
                            <th><span style="white-space: pre-wrap;">Old variable</span></th>
                            <th><span style="white-space: pre-wrap;">New variable</span></th>
                        </tr>
                        {rows}
                    </tbody>
                </table>
            </div>
            """
            row_template = """
            <tr>
                <td>
                    {cell_old}
                </td>
                <td>
                    {cell_new}
                </td>
            </tr>
            """
            link_template = """
            <a href="{link}" target="_blank">{title}</a>
            """
            cell_template = f"""
            <details>
                <summary>{link_template}</summary>
                {{summary}}
            </details>
            """
            rows = []
            for old_id, new_id in mapping.items():
                # Old variable
                summary = self._build_variable_description(old_id)
                if summary:
                    cell_old = cell_template.format(
                        link=OWID_ENV.variable_admin_url(old_id),
                        title=f"{self.variable_id_to_name.get(old_id)} ({old_id})",
                        summary=summary,
                    )
                else:
                    cell_old = link_template.format(
                        link=OWID_ENV.variable_admin_url(old_id),
                        title=f"{self.variable_id_to_name.get(old_id)} ({old_id})",
                    )
                # New variable
                if new_id == -1:
                    cell_new = "Ignore"
                else:
                    summary = self._build_variable_description(new_id)
                    if summary:
                        cell_new = cell_template.format(
                            link=OWID_ENV.variable_admin_url(new_id),
                            title=f"{self.variable_id_to_name.get(new_id)} ({new_id})",
                            summary=summary,
                        )
                    else:
                        cell_new = link_template.format(
                            link=OWID_ENV.variable_admin_url(new_id),
                            title=f"{self.variable_id_to_name.get(new_id)} ({new_id})",
                        )
                row = row_template.format(cell_old=cell_old, cell_new=cell_new)
                rows.append(row)
            rows = "".join(rows)
            po.put_html(table.format(rows=rows))

        if separate:
            if self.variable_mapping_manual:
                po.put_markdown("### Manually mapped:")
                _show_table(self.variable_mapping_manual)
            if self.variable_mapping_auto:
                po.put_markdown(
                    "### Automatically mapped:\nThese are variables that were automatically mapped by the tool because"
                    " their names were identical. To disable this, re-run this step and uncheck 'Pair identical"
                    " variables' option."
                )
                _show_table(self.variable_mapping_auto)
        else:
            _show_table(self.variable_mapping)

    def _build_variable_description(self, id_: int):
        """Create html code that describes variables.

        This html code comes with each variable, typically shown as a dropdown.
        """
        if id_ == -1:
            return None
        items = []
        timespan = self.variable_id_to_all[id_].get("timespan")
        if timespan:
            items.append(
                f"""
            <div><b>Timespan:</b> {timespan}</div>
            """
            )
        unit = self.variable_id_to_all[id_].get("unit")
        if unit:
            items.append(
                f"""
            <div><b>Unit: </b>{unit}</div>
            """
            )
        coverage = self.variable_id_to_all[id_].get("coverage")
        if coverage:
            items.append(
                f"""
            <div><b>Coverage:</b> {coverage}</div>
            """
            )
        description = self.variable_id_to_all[id_].get("description")
        if description:
            items.append(
                f"""
            <div><b>Description:</b> {description}</div>
            """
            )
        # Create text description
        if items:
            items = "".join(items)
            text = f"""
            <div class=table-variable-container>
                {items}
            </div>
            """
            return text
        else:
            return None

    def _varid_to_all(self) -> Dict[int, Dict[str, Any]]:
        """Build mapping from variable ID to all variable's fields."""
        log.info(f"Mapping IDs to names: {self.variable_mapping}")
        # get mapping from db
        with get_connection() as db_conn:
            var_ids = list(self.variable_mapping.keys()) + list(self.variable_mapping.values())
            var_ids = str(tuple(set(var_ids)))
            query = f"""
                SELECT *
                FROM variables
                WHERE id in {var_ids}
            """
            df = pd.read_sql(query, db_conn)
            mapping = df.set_index("id").to_dict(orient="index")
        log.info("Found mapping!")  #: {mapping}")
        return mapping

    def show_submission_details(self, suggester: ChartRevisionSuggester) -> Union[List[dict[str, Any]], None]:
        """Show submission details.

        This includes the variable id mapping, but also the charts that will be affected by the mapping.
        """
        log.info("Showing submission details...")
        # get ID mapping without ignore ones (-1)
        po.toast("Getting submission details...")
        po.put_markdown("## Submission details")
        if not self.variable_mapping:
            po.put_error("Mapping is empty!")
        else:
            po.put_markdown("### Variable ID mapping to be submitted")
            po.put_code(self.variable_mapping, "json")
            po.put_markdown("### Charts affected")
            po.put_processbar("bar_submitting_charts", auto_close=True)
            try:
                suggested_chart_revisions = []
                num_charts = len(suggester.df_charts)
                for i, row in enumerate(suggester.df_charts.itertuples()):
                    revision = suggester.prepare_chart_single(row)
                    if revision:
                        suggested_chart_revisions.append(revision)
                    po.set_processbar("bar_submitting_charts", i / num_charts)
                po.set_processbar("bar_submitting_charts", 1)
            except Exception as e:
                po.put_error(f"Error: {e}")
                return
            else:
                # short summary
                if len(suggested_chart_revisions) == 0:
                    po.put_markdown("No charts affected by this variable mapping!")
                else:
                    po.put_markdown(
                        f"There are **{len(suggested_chart_revisions)} charts** that will be affected by the mapping:"
                    )
                    # chart details
                    _show_chart_details(suggested_chart_revisions)
                # logs
                if suggester.logs:
                    _show_logs_from_suggester(suggester.logs)
            return suggested_chart_revisions

    def show_submission_details_exp(
        self, revisions: List[ChartVariableUpdateRevision]
    ) -> Optional[List[ChartVariableUpdateRevision]]:
        """Show submission details.

        This includes the variable id mapping, but also the charts that will be affected by the mapping.
        """
        log.info("Showing submission details...")
        # get ID mapping without ignore ones (-1)
        po.toast("Getting submission details...")
        po.put_markdown("## Submission details")
        if not self.variable_mapping:
            po.put_error("Mapping is empty!")
        else:
            po.put_markdown("### Variable ID mapping to be submitted")
            po.put_code(self.variable_mapping, "json")
            po.put_markdown("### Charts affected")
            po.put_processbar("bar_submitting_charts", auto_close=True)
            try:
                revisions_to_submit = []
                num_revisions = len(revisions)
                for i, revision in enumerate(revisions):
                    revision_ = revision.bake()
                    # If config has actually changed, then add to list to submit
                    if revision_.config_has_changed:
                        revisions_to_submit.append(revision_)
                    po.set_processbar("bar_submitting_charts", i / num_revisions)
                po.set_processbar("bar_submitting_charts", 1)
            except Exception as e:
                po.put_error(f"Error: {e}")
                return
            else:
                # short summary
                if len(revisions_to_submit) == 0:
                    po.put_markdown("No charts affected by this variable mapping!")
                else:
                    po.put_markdown(
                        f"There are **{len(revisions_to_submit)} charts** that will be affected by the mapping:"
                    )
                    # chart details
                    _show_chart_details(revisions_to_submit)
                    # logs
                    logs = []
                    for revision in revisions_to_submit:
                        logs += revision._logs
                    if logs:
                        _show_logs_from_suggester(logs)
            return revisions_to_submit

    def submit_suggestions(
        self, suggester: ChartRevisionSuggester, suggested_chart_revisions: List[dict[str, Any]]
    ) -> int:
        """Submit suggestinos to Grapher.

        If successfull, a green box with success message is shown. Otherwise, red box with error message is shown.
        """
        log.info("Submitting suggestions to Grapher...")
        po.put_markdown("## Submission to Grapher")
        po.put_markdown("### Grapher response")

        with po.put_loading():
            po.put_text("Submitting to Grapher...")
            try:
                suggester.insert(suggested_chart_revisions=suggested_chart_revisions)
            except Exception as e:
                po.put_error(e)
                po.toast("Something went wrong when submitting suggestions to Grapher!", color="error")
                approval_chart_link = OWID_ENV.chart_approval_tool_url
                po.put_markdown(
                    f"Check on the [Chart approval tool]({approval_chart_link})! You might need to delete old chart"
                    " revisions. Then, run `walkthrough charts` step again."
                )
                return -1
            else:
                msg = "Chart revisions have been submitted to Grapher!"
                po.toast(msg, color="success")
                po.put_success(msg)
        return 0

    def submit_suggestions_exp(self, revisions: List[ChartVariableUpdateRevision]) -> int:
        """Submit suggestinos to Grapher.

        If successfull, a green box with success message is shown. Otherwise, red box with error message is shown.
        """
        log.info("Submitting suggestions to Grapher...")
        po.put_markdown("## Submission to Grapher")
        po.put_markdown("### Grapher response")

        with po.put_loading():
            po.put_text("Submitting to Grapher...")
            try:
                submit_revisions_to_grapher(revisions)
            except Exception as e:
                po.put_error(e)
                po.toast("Something went wrong when submitting suggestions to Grapher!", color="error")
                approval_chart_link = OWID_ENV.chart_approval_tool_url
                po.put_markdown(
                    f"Check on the [Chart approval tool]({approval_chart_link})! You might need to delete old chart"
                    " revisions. Then, run `walkthrough charts` step again."
                )
                return -1
            else:
                msg = "Chart revisions have been submitted to Grapher!"
                po.toast(msg, color="success")
                po.put_success(msg)
        return 0

    def _clean_variable_mapping(self):
        log.info("Cleaning variable mapping...")
        self.variable_mapping_manual = {k: v for k, v in self.variable_mapping_manual.items() if v != -1}


def _show_chart_details(revisions):
    log.info("Showing chart details...")
    iframe = """
    <iframe
        src="https://ourworldindata.org/grapher/{}"
        loading="lazy"
        style="width: 100%; height: 600px; border: 0px none;"
    ></iframe>
    """
    po.put_scrollable(po.put_scope("scroll-charts"), height=800)
    revisions = sorted(revisions, key=lambda x: x["chartSlug"].lower())
    for revision in revisions:
        slug = revision["chartSlug"]
        title = slug.replace("-", " ").capitalize()
        po.put_collapse(title=title, content=[po.put_html(iframe.format(slug))], scope="scroll-charts")


def _show_logs_from_suggester(logs):
    log.info("Showing logs...")
    try:
        po.put_markdown("#### Logs")
        po.put_scrollable(po.put_scope("scroll-logs"))
        for msg in logs:
            text = msg["message"]
            match = re.search(r"([Cc]hart (\d+)).*", text)
            if match:
                text_repl = match.group(1)
                chart_id = match.group(2)
                text = text.replace(text_repl, f"<a href='{OWID_ENV.chart_admin_url(chart_id)}'>{text_repl}</a>")
            html = po.put_html(text)
            if msg["type"] == "error":
                po.put_error(html, scope="scroll-logs")
            elif msg["type"] == "warning":
                po.put_warning(html, scope="scroll-logs")
            elif msg["type"] == "info":
                po.put_info(html, scope="scroll-logs")
            elif msg["type"] == "success":
                po.put_success(html, scope="scroll-logs")
    except Exception as e:
        po.put_error(
            po.put_html(
                "There was an error while retrieving the logs. Please report <a"
                f" href='https://github.com/owid/etl/issues/new'>here</a>! Complete error trace: {e}"
            )
        )
    else:
        po.toast("Submission details available!", color="success")

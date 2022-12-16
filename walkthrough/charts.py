from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po

import etl
from etl import config
from etl.match_variables_from_two_versions_of_a_dataset import (
    SIMILARITY_NAME,
    SIMILARITY_NAMES,
    preliminary_mapping,
    find_mapping_suggestions,
    get_similarity_function,
)
from etl.chart_revision_suggester import ChartRevisionSuggester
from etl.db import get_connection, get_dataset_id, get_variables_in_dataset


from . import utils

CURRENT_DIR = Path(__file__).parent

ETL_DIR = Path(etl.__file__).parent.parent


class Options(Enum):

    ADD_TO_DAG = "Add steps into dag.yml file"
    IS_PRIVATE = "Make dataset private"


class GrapherForm(BaseModel):

    short_name: str
    namespace: str
    version: str
    add_to_dag: bool
    is_private: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["add_to_dag"] = Options.ADD_TO_DAG.value in options
        data["is_private"] = Options.IS_PRIVATE.value in options
        super().__init__(**data)


old_ids = [
    "old_1",
    "old_2",
]
new_ids = [
    ["new_1", "new_2"],
    ["new_3", "new_4"],
]


def app(run_checks: bool, dummy_data: bool) -> None:

    # show initial description
    with open(CURRENT_DIR / "charts.md", "r") as f:
        po.put_markdown(f.read())

    # show environment
    if run_checks:
        _check_env()
    _show_environment()

    # ask user for dataset names
    data = pi.input_group(
        "Options",
        [
            pi.input(
                "Old dataset name",
                name="old_dataset",
                value="Life expectancy - Riley (2005), Clio Infra (2015), and UN (2019)",
                placeholder="United Nations - Population Division (2022)",
                required=True,
            ),
            pi.input(
                "New dataset name",
                name="new_dataset",
                value="Life Expectancy (various sources)",
                placeholder="United Nations - Population Division (2024)",
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
    po.put_info(
        po.put_markdown(
            f"""
        * **Old dataset**: *{data['old_dataset']}*
        * **New dataset**: *{data['new_dataset']}*
        * **Similarity matching function**: {data['similarity_function']}
        * Pairing identically named variables is **{'enabled' if data['pair_identical'] else 'disabled'}**
        """
        )
    )

    # get suggestions
    run_variable_mapping_selection(data)

    # submit suggestions
    submit = pi.actions(label="Confirm to submit variable mapping to Grapher?", buttons=["Yes"])

    if submit == "Yes":
        submit_suggestions()


def submit_suggestions():
    with po.put_loading():
        print("Submitting suggestions to Grapher...")


def run_variable_mapping_selection(data):
    # get suggestions
    suggestions = _get_suggestions(
        data["old_dataset"], data["new_dataset"], data["similarity_function"], data["pair_identical"]
    )
    # get the user to map old to new variables
    po.put_markdown("## Mapping old to new variables")
    mapping = {}
    mapping_names = {}
    for _, suggestion in enumerate(suggestions):
        old_varname = suggestion["old"]["name_old"]
        old_id = suggestion["old"]["id_old"]
        variables_new = suggestion["new"]

        new_id = pi.select(
            old_varname,
            options=[
                {"label": f"{v['name_new']} ({v['similarity']} %)", "value": v["id_new"]}
                for _, v in variables_new.iterrows()
            ],
            help_text=f"Select the new variable name for '{old_varname}'. Matching score is given in brackets.",
            required=True,
            # name=str(old_id),
        )
        mapping[old_id] = new_id
        mapping_names[old_varname] = variables_new.loc[variables_new["id_new"] == new_id, "name_new"].values[0]
    # show results
    tdata = [["Old variable", "New variable"]] + [[old, new] for old, new in mapping_names.items()]
    po.put_table(tdata)


def _get_suggestions(old_dataset_name, new_dataset_name, similarity_name, omit_identical):
    with get_connection() as db_conn:
        # Get old and new dataset ids.
        old_dataset_id = get_dataset_id(db_conn=db_conn, dataset_name=old_dataset_name)
        new_dataset_id = get_dataset_id(db_conn=db_conn, dataset_name=new_dataset_name)

        # Get variables from old dataset that have been used in at least one chart.
        old_variables = get_variables_in_dataset(db_conn=db_conn, dataset_id=old_dataset_id, only_used_in_charts=True)
        # Get all variables from new dataset.
        new_variables = get_variables_in_dataset(db_conn=db_conn, dataset_id=new_dataset_id, only_used_in_charts=False)

    # Select similarity function.
    similarity_function = get_similarity_function(similarity_name)
    # get initial mapping
    mapping, missing_old, missing_new = preliminary_mapping(old_variables, new_variables, omit_identical)
    # get suggestions for mapping
    suggestions = find_mapping_suggestions(missing_old, missing_new, similarity_function)

    return suggestions


def _check_env() -> None:
    po.put_markdown("""## Checking `.env` file...""")

    ok = True
    for env_name in ("GRAPHER_USER_ID", "DB_USER", "DB_NAME", "DB_HOST"):
        if getattr(config, env_name) is None:
            ok = False
            po.put_warning(
                po.put_markdown(f"Environment variable `{env_name}` not found, do you have it in your `.env` file?")
            )

    if ok:
        po.put_success(po.put_markdown("`.env` configured correctly"))


def _show_environment() -> None:
    po.put_markdown("## Environment")
    po.put_info(
        po.put_markdown(
            f"""
    * **GRAPHER_USER_ID**: `{config.GRAPHER_USER_ID}`
    * **DB_USER**: `{config.DB_USER}`
    * **DB_NAME**: `{config.DB_NAME}`
    * **DB_HOST**: `{config.DB_HOST}`
    """
        )
    )

"""Garden phase."""

import glob
import os
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st
from rapidfuzz import fuzz
from sqlalchemy.exc import OperationalError

import etl.grapher.model as gm
from apps.wizard import utils
from apps.wizard.etl_steps.forms import DataForm
from apps.wizard.etl_steps.instructions import render_instructions
from apps.wizard.etl_steps.utils import STEP_NAME_PRESENT, TAGS_DEFAULT, dag_files, dag_not_add_option
from apps.wizard.utils.components import config_style_html, preview_file, st_horizontal, st_multiselect_wider
from etl.config import DB_HOST, DB_NAME
from etl.db import get_session
from etl.paths import DATA_DIR, SNAPSHOTS_DIR

# from etl.snapshot import Snapshot

#########################################################
# CONSTANTS #############################################
#########################################################
st.set_page_config(
    page_title="Wizard: Data Step",
    page_icon="🪄",
)
st.session_state.submit_form = st.session_state.get("submit_form", False)
st.session_state["data.steps_to_create"] = st.session_state.get("data.steps_to_create", [])
st.session_state["data_steps_to_create"] = st.session_state.get("data_steps_to_create")
st.session_state.update_steps_selection = st.session_state.get("update_steps_selection", False)
st.session_state.data_edit_namespace_sname_version = st.session_state.get("data_edit_namespace_sname_version", False)

# Available namespaces
OPTIONS_NAMESPACES = utils.get_namespaces("all")


# Get current directory
CURRENT_DIR = Path(__file__).parent
# FIELDS FROM OTHER STEPS
st.session_state["step_name"] = "data"
APP_STATE = utils.AppState()
APP_STATE._previous_step = "snapshot"
# Config style
config_style_html()
# DUMMY defaults
dummy_values = {
    "namespace": "dummy",
    "version": utils.DATE_TODAY,
    "short_name": "dummy",
    "topic_tags": ["Uncategorized"],
}

# Get list of available tags from DB (only those used as topic pages)
# If can't connect to DB, use TAGS_DEFAULT instead
USING_TAGS_DEFAULT = False
try:
    with get_session() as session:
        tag_list_ = gm.Tag.load_tags(session)
        tag_list = ["Uncategorized"] + sorted([tag.name for tag in tag_list_])
except OperationalError:
    USING_TAGS_DEFAULT = True
    tag_list = TAGS_DEFAULT
# NOTE: Use this when debugging
# USING_TAGS_DEFAULT = True
# tag_list = TAGS_DEFAULT


# GET SNAPSHOT URIS
def get_snapshots():
    base_folder = DATA_DIR / "snapshots"
    # Construct a glob pattern for level 3 files
    pattern = os.path.join(base_folder, "*/*/*")

    # Use glob to get all file paths matching the pattern
    snapshots = [
        f"snapshot://{os.path.relpath(path, base_folder)}" for path in glob.glob(pattern) if os.path.isfile(path)
    ]

    # Get list of private ones
    bash_cmd = rf'grep -rl --exclude-dir="backport" -E "is_public\s*:\s*false" {SNAPSHOTS_DIR}'
    result = subprocess.run(bash_cmd, shell=True, capture_output=True, text=True)
    steps_private = result.stdout.split("\n")
    steps_private = [
        step.replace(str(SNAPSHOTS_DIR), "snapshot:/").replace(".dvc", "") for step in steps_private if step != ""
    ]

    # Get list of archived ones
    # archived = utils.get_datasets_in_etl(dag_path=DAG_ARCHIVE_FILE, snapshots=True)
    # archived = [s for s in archived if re.match(r"^snapshot(-private)?://", s)]

    # Combine all
    snapshots = [
        s.replace("snapshot://", "snapshot-private://") if s in steps_private else s
        for s in snapshots
        # if s not in archived
    ]
    return snapshots


@st.cache_data
def get_snapshots_cached():
    return get_snapshots()


st.session_state["snapshot_uris"] = st.session_state.get("snapshot_uris", get_snapshots_cached())


@st.cache_data
def get_steps_per_channel():
    steps_all = utils.get_datasets_in_etl(
        snapshots=True,
    )
    steps = {"garden": [], "grapher": []}
    for s in steps_all:
        # if re.match(r"^(data(-private)?://meadow|snapshot(-private)?://)", s):
        #     steps["meadow"].append(s)
        if re.match(r"^(data(-private)?://(meadow|garden))", s):
            steps["garden"].append(s)
        if re.match(r"^(data(-private)?://(garden|grapher))", s):
            steps["grapher"].append(s)

    return steps


# GET STEP URIS
STEPS_URI = get_steps_per_channel()


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################


def submit_form() -> None:
    """Submit form."""
    # Create form
    form = DataForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)

    # Submit
    utils.set_states({"submit_form": True})


def edit_field() -> None:
    """Submit form."""
    utils.set_states({"submit_form": False})


def edit_dependant_field() -> None:
    """Submit form."""
    utils.set_states(
        {
            "submit_form": False,
            "data_edit_namespace_sname_version": True,
        }
    )


def render_step_selection():
    """Render step selection."""
    # st.write(st.session_state)
    with st_horizontal(vertical_alignment="center"):  # ("center", justify_content="space-between"):
        # Multi-select (data steps)
        if (st.session_state.update_steps_selection) and (st.session_state["data_steps_to_create"] is not None):
            st.session_state["data.steps_to_create"] = ["meadow", "garden", "grapher"]
            st.session_state.update_steps_selection = False

        st.segmented_control(
            label="Select the steps you want to create",
            options=["meadow", "garden", "grapher"],
            format_func=lambda option: STEP_NAME_PRESENT[option],
            selection_mode="multi",
            help="Select the steps that you want to create. You can choose as many as necessary.",
            key="data.steps_to_create",
            on_change=edit_field,
        )
        # Express mode
        if (st.session_state["data_steps_to_create"] is not None) and (
            len(st.session_state["data.steps_to_create"]) != 3
        ):
            st.session_state["data_steps_to_create"] = None

        # Set to false if: data_steps_to_create is "express" & len(st.session_state["data.steps_to_create"]) is not 3
        st.segmented_control(
            "Express?",
            ["express"],
            format_func=lambda _: ":material/bolt: Use express mode",
            label_visibility="hidden",
            help="Express mode will create all steps at once.",
            # default=default,
            key="data_steps_to_create",
            on_change=lambda: utils.set_states(
                {"update_steps_selection": True, "submit_form": False}  # , "data_edit_namespace_sname_version": True}
            ),
        )

        if len(st.session_state["data.steps_to_create"]) > 0:
            return True
        return False


def render_form_main():
    """Render main part of the form."""
    col1, col2, col3 = st.columns([2, 2, 1])
    #
    # Namespace
    #
    with col1:
        custom_label = "Custom namespace..."
        APP_STATE.st_selectbox_responsive(
            st_widget=st.selectbox,
            custom_label=custom_label,
            key="namespace",
            label="Namespace",
            help="Institution or topic name",
            options=OPTIONS_NAMESPACES,
            default_last=dummy_values["namespace"] if APP_STATE.args.dummy_data else OPTIONS_NAMESPACES[0],
            on_change=edit_field,
        )
        if APP_STATE.vars.get("namespace") == custom_label:
            namespace_key = "namespace_custom"
        else:
            namespace_key = "namespace"

    #
    # Short name (meadow, garden, grapher)
    #
    with col2:
        APP_STATE.st_widget(
            st_widget=st.text_input,
            key="short_name",
            label="short name",
            help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
            placeholder="Example: 'cherry_blossom'",
            value=dummy_values["short_name"] if APP_STATE.args.dummy_data else None,
            on_change=edit_field,
        )

    #
    # Version (meadow, garden, grapher)
    #
    if (default_version := APP_STATE.default_value("version", previous_step="snapshot")) == "":
        default_version = APP_STATE.default_value("snapshot_version", previous_step="snapshot")
    with col3:
        APP_STATE.st_widget(
            st_widget=st.text_input,
            label="version",
            help="Version of the dataset (by default, the current date, or exceptionally the publication date).",
            key="version",
            default_last=default_version,
            value=dummy_values["version"] if APP_STATE.args.dummy_data else default_version,
            on_change=edit_field,
        )

    #
    # Add to DAG
    #
    sorted_dag = sorted(
        dag_files,
        key=lambda file_name: fuzz.ratio(file_name.replace(".yml", ""), APP_STATE.vars[namespace_key]),
        reverse=True,
    )
    sorted_dag = [
        dag_not_add_option,
        *sorted_dag,
    ]
    if sorted_dag[1].replace(".yml", "") == APP_STATE.vars[namespace_key]:
        default_value = sorted_dag[1]
    else:
        default_value = ""

    APP_STATE.st_widget(
        st.selectbox,
        label="Add to DAG",
        options=sorted_dag,
        key="dag_file",
        help="Add ETL step to a DAG file. This will allow it to be tracked and executed by the `etl` command.",
        default_value=default_value,
        on_change=edit_field,
    )

    #
    # Garden options: update_frequency, tags
    if "garden" in st.session_state["data.steps_to_create"]:
        col1, col2 = st.columns(2)
        #
        # Indicator tags
        #
        with col1:
            label = "Indicators tag"
            if USING_TAGS_DEFAULT:
                label += f"\n\n:red[Using a 2025 February snapshot of the tags. Couldn't connect to database `{DB_NAME}` in host `{DB_HOST}`.]"

            namespace = APP_STATE.vars[namespace_key].replace("_", " ")
            default_last = None
            for tag in tag_list:
                if namespace.lower() == tag.lower():
                    default_last = tag
                    break
            APP_STATE.st_widget(
                st_widget=st.multiselect,
                label=label,
                help=(
                    """
                    This tag will be propagated to all dataset's indicators (it will not be assigned to the dataset).

                    If you want to use a different tag for a specific indicator you can do it by editing its metadata field `variable.presentation.topic_tags`.

                    Exceptionally, and if unsure what to choose, choose tag `Uncategorized`.
                    """
                ),
                key="topic_tags",
                options=tag_list,
                placeholder="Choose a tag (or multiple)",
                default=dummy_values["topic_tags"] if APP_STATE.args.dummy_data else default_last,
                on_change=edit_field,
            )

        #
        # Update frequency
        #
        with col2:
            today = datetime.today()
            APP_STATE.st_widget(
                st_widget=st.date_input,
                label="When is the next update expected?",
                help="Expected date of the next update of this dataset by OWID (typically in a year).",
                key="update_period_date",
                min_value=today + timedelta(days=1),
                default_last=today.replace(year=today.year + 1),
                on_change=edit_field,
            )

    return namespace_key


@st.fragment
def render_form_dependencies(namespace_key):
    if "meadow" in st.session_state["data.steps_to_create"]:
        with st_horizontal(vertical_alignment="center", justify_content="space-between"):
            st.markdown("#### Dependencies")
            st.button(
                ":material/refresh: Refresh snapshot list",
                type="tertiary",
                help="The following dropdown only shows snapshots that have been added to the catalog (local folder `data/`). If a snapshot is missing, please add it to the catalog first (i.e. `python snapshot ...`), and then click here to refresh the list.",
                on_click=lambda: utils.set_states({"snapshot_uris": get_snapshots()}),
            )

        # Render snapshot selector
        render_snapshot_selection(namespace_key)

    if any(step in st.session_state["data.steps_to_create"] for step in ["garden", "grapher"]):
        st.markdown("###### OPTIONAL extra dependencies")
        for channel in ["garden", "grapher"]:
            if channel in st.session_state["data.steps_to_create"]:
                APP_STATE.st_widget(
                    st_widget=st.multiselect,
                    label=f"Extra dependencies for {channel.capitalize()}",
                    help="Additional dependencies.",
                    key=f"data.dependencies_extra_{channel}",
                    options=STEPS_URI[channel],
                    placeholder="Add dependency steps",
                    default=None,
                    on_change=edit_field,
                )


def render_snapshot_selection(namespace_key):
    def similarity_snap(uri):
        # Extract individual URI parts
        pattern = r"(?:snapshot(?:-private)?://)([^/]+)/([^/]+)/([^/.]+)\.\w+"
        match = re.match(pattern, uri)
        if match:
            namespace, version, short_name = match.groups()
        else:
            st.toast(f"Error parsing snapshot URI: {uri}. Please report.", icon="🚨")
            return 0

        # Compare with current values
        if namespace == APP_STATE.vars[namespace_key]:
            namespace_score = 150
        else:
            namespace_score = fuzz.ratio(namespace, APP_STATE.vars[namespace_key])
        if short_name == APP_STATE.vars["short_name"]:
            short_name_score = 150
        else:
            short_name_score = fuzz.ratio(short_name, APP_STATE.vars["short_name"])
        if version == APP_STATE.vars["version"]:
            version_score = 150
        elif version == "latest":
            version_score = 100
        elif bool(re.match(r"^\d{4}-\d{2}-\d{2}$", version)):
            date_obj = datetime.strptime(version, "%Y-%m-%d").date()
            delta = (datetime.today().date() - date_obj).days
            if delta < 0:
                version_score = 100
            else:
                version_score = max(1 - delta / 4_000, 0) * 100
        else:
            version_score = 0

        score = namespace_score + short_name_score + 0.2 * version_score

        return score

    sorted_snaps = sorted(
        st.session_state["snapshot_uris"],
        key=lambda uri: similarity_snap(uri),
        reverse=True,
    )

    # default = None
    # if st.session_state["data_edit_namespace_sname_version"] and ("data.snapshot_dependencies" in st.session_state):
    #     default = APP_STATE.vars["snapshot_dependencies"]
    #     st.session_state["data_edit_namespace_sname_version"] = False

    def render_snapshot_selection_widget():
        """Use fragment to avoid flickering"""
        st.session_state["snapshot_dependencies_saving"] = APP_STATE.st_widget(
            st.multiselect,
            label="Snapshots",
            help="Select snapshots.",
            placeholder="Select snapshots",
            options=sorted_snaps,
            default=st.session_state.get("data.snapshot_dependencies", None),
            key="snapshot_dependencies",
            on_change=edit_field,
        )

    render_snapshot_selection_widget()


@st.fragment
def render_form_others():
    st.markdown("#### Other options")
    name_mapping = {
        "private": ":material/lock:  Make dataset private",
        "notebook": ":material/skateboarding:  Create playground notebook",
    }

    st.pills(
        label="Extra options.",
        help="Extra options.",
        options=["private", "notebook"],
        format_func=lambda option: name_mapping[option],
        selection_mode="multi",
        label_visibility="collapsed",
        key="data.extra_options",
        on_change=edit_field,
    )


@st.fragment
def render_form():
    """Render form."""
    #
    # Main options
    #
    with st.container(border=True):
        namespace_key = render_form_main()

    #
    # Meadow options: pick snapshot
    #
    with st.container(border=True):
        render_form_dependencies(namespace_key)

    #
    # Others
    #
    with st.container(border=True):
        render_form_others()


#########################################################
# MAIN ##################################################
#########################################################
st_multiselect_wider()
# TITLE
st.title(":material/bolt: Data **:gray[Create steps]**")

# SELECT MODE
step_selected = render_step_selection()

# FORM
if step_selected:
    # form_widget = st.empty()
    render_form()
    st.button(
        label="Submit",
        type="primary",
        use_container_width=True,
        on_click=submit_form,
    )
else:
    st.warning("Select at least one step to create.")

with st.sidebar:
    with st.popover("Preview instructions after generating files"):
        st.markdown(
            "This is a preview of the instructions that will be shown after generating the files. It uses generic namings like `namespace`, `short_name`, etc. The instructions with actual valid names are shown after submitting the form."
        )
        render_instructions(key="auxiliary")

#########################################################
# SUBMISSION ############################################
#########################################################
if st.session_state.submit_form:
    # Create form
    form = DataForm.from_state()

    # st.write(form.model_dump())
    if not form.errors:
        # Remove form from UI
        # form_widget.empty()

        # Create files for all steps
        generated_files = {}
        for channel in form.steps_to_create:
            generated_files_ = form.create_files(channel)
            generated_files[channel] = generated_files_

        # Add lines to DAG
        dag_content = form.add_steps_to_dag()

        ########################
        # PREVIEW & NEXT STEPS #
        ########################
        utils.preview_dag_additions(dag_content, form.dag_path, prefix="**DAG**", expanded=True)

        # st.write(generated_files)
        tab_instructions, tab_files = st.tabs(["Instructions", "Files"])

        with tab_files:
            for channel in ["meadow", "garden", "grapher"]:
                if channel in generated_files:
                    # st.markdown(f"**{channel.title()}**")
                    for f in generated_files[channel]:
                        preview_file(f["path"], f"**{STEP_NAME_PRESENT.get(channel)}**", f["language"])

        with tab_instructions:
            render_instructions(form)

        # Prompt user
        st.toast("Templates generated. Read the next steps.", icon="✅")

        # Update config
        utils.update_wizard_defaults_from_form(form=form)
    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")


# st.divider()
# st.subheader("Legacy")
# st_wizard_page_link("meadow")
# st_wizard_page_link("garden")
# st_wizard_page_link("grapher")

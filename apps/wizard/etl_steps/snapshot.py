"""Snapshot phase."""

import subprocess
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import streamlit as st

from apps.utils.files import generate_step
from apps.wizard import utils
from apps.wizard.etl_steps.forms import SnapshotForm
from apps.wizard.etl_steps.utils import COOKIE_SNAPSHOT, MD_SNAPSHOT, SCHEMA_ORIGIN
from apps.wizard.utils.components import preview_file, st_wizard_page_link
from etl.docs import examples_to_markdown, faqs_to_markdown, guidelines_to_markdown
from etl.paths import BASE_DIR, SNAPSHOTS_DIR

#########################################################
# CONSTANTS #############################################
#########################################################
st.set_page_config(
    page_title="Wizard: Snapshot",
    page_icon="🪄",
)
# Lists with fields of special types. By default, fields are text inputs.
FIELD_TYPES_TEXTAREA = [
    "origin.description_snapshot",
    "origin.description",
    "origin.citation_full",
]
FIELD_TYPES_SELECTBOX = [
    "origin.attribution",
    "origin.license.name",
]
# Get current directory
CURRENT_DIR = Path(__file__).parent
# Accepted schema categories
ACCEPTED_CATEGORIES = ["dataset", "citation", "files", "license"]
# Available namespaces (ignore hidden files)
OPTIONS_NAMESPACES = sorted(f.name for f in SNAPSHOTS_DIR.glob("[!.]*"))
# FIELDS FROM OTHER STEPS
st.session_state["step_name"] = "snapshot"
APP_STATE = utils.AppState()
# DUMMY defaults
dummy_values = {
    "namespace": "dummy",
    "snapshot_version": utils.DATE_TODAY,
    "short_name": "dummy",
    "file_extension": "csv",
    # Programatically generated based on schemas/
    "origin.title": "Dummy data product",
    "origin.date_published": utils.DATE_TODAY,
    "origin.producer": "Non-dummy producer",
    "origin.citation_full": "Dummy description for a dummy snapshot.",
    "origin.url_main": "https://dummy.dummy",
    "origin.date_accessed": utils.DATE_TODAY,
}

# Other state vars
if "show_form" not in st.session_state:
    st.session_state["show_form"] = True
if "run_step" not in st.session_state:
    st.session_state["run_step"] = False
if "snapshot_file" not in st.session_state:
    st.session_state["snapshot_file"] = None


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################


def _add_dummy_default(
    args, key: str, default_colname: Optional[str] = None, default_value: Optional[Any] = None
) -> None:
    if default_colname is None:
        default_colname = "value"
    if APP_STATE.args.dummy_data:
        args[default_colname] = dummy_values.get(key, default_value)


def _color_req_level(req_level: str) -> str:
    colored_level = req_level
    if req_level == "required":
        return f":red[{colored_level}]"
    elif "required" in req_level:
        colored_level = colored_level.replace("required", ":red[required]")
    elif "recommended" in req_level:
        colored_level = colored_level.replace("recommended", ":blue[recommended]")
    elif "optional" in req_level:
        colored_level = "optional"
    else:
        raise ValueError(f"Unknown requirement level: {req_level}")
    return colored_level


def create_display_name_init_section(name: str) -> str:
    """Create display name for a field."""
    # Get requirement level colored
    req_level = _color_req_level("required")
    # Create display name
    display_name = f"**{name}** {req_level}"
    return display_name


def create_display_name_snap_section(
    props: Dict[str, Any], name: str, property_name: str, title: Optional[str] = None
) -> str:
    """Create display name for a field."""
    # Get requirement level colored
    req_level = _color_req_level(props["requirement_level"])
    # Create display name
    if not title:
        title = props["title"]
    # display_name = f"`{property_name}.{name}` ┃ {title} ┃ {req_level}"
    # display_name = f"{property_name}.{name}, {req_level}"
    display_name = f"**{property_name}.{name}** {req_level}".replace(".", "/")
    return display_name


@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(file=MD_SNAPSHOT, mode="r") as f:
        return f.read()


def create_description(field: Dict[str, Any]) -> str:
    """Create description for field, using values `description` and `guidelines`."""
    # Main description
    toc = "[Description](#description) "
    description = f"## Description\n\n {field['description']}"
    # Guidelines
    if field.get("guidelines"):
        description += "\n## Guidelines\n\n" + guidelines_to_markdown(guidelines=field["guidelines"], extra_tab=0)
        toc += "| [Guidelines](#guidelines) "
    # Examples (good vs bad)
    if field.get("examples"):
        if "examples_bad" in field:
            description += "\n## Examples\n\n" + examples_to_markdown(
                examples=field["examples"],
                examples_bad=field["examples_bad"],
                extra_tab=0,
                do_sign="✅",
                dont_sign="❌",
            )
        else:
            description += "\n## Examples\n\n" + examples_to_markdown(
                examples=field["examples"], examples_bad=[], extra_tab=0, do_sign="✅", dont_sign="❌"
            )
        toc += "| [Examples](#examples) "
    # FAQs
    if field.get("faqs"):
        description += "\n## FAQs\n\n" + faqs_to_markdown(faqs=field["faqs"], extra_tab=0)
        toc += "| [FAQs](#faqs) "
    # Insert TOC at the beginnining of description
    description = toc.strip() + "\n\n" + description
    return description


def render_fields_init():
    """Render fields to create directories and all."""
    col1, col2, col3 = st.columns([2, 2, 1], vertical_alignment="bottom")

    # namespace
    with col1:
        prop_uri = "namespace"
        args = {
            "st_widget": st.selectbox,
            "label": create_display_name_init_section("Namespace"),
            "options": OPTIONS_NAMESPACES,
            "help": "## Description\n\nInstitution or topic name",
            "key": prop_uri,
            "default_last": OPTIONS_NAMESPACES[0],
            "accept_new_options": True,
        }
        _add_dummy_default(args, prop_uri, "default_last")
        APP_STATE.st_widget(**args)

    # short name
    with col2:
        key = "short_name"
        args = {
            "st_widget": st.text_input,
            "key": key,
            "label": create_display_name_init_section("short name"),
            "help": "## Description\n\nDataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
            "placeholder": "Example: 'cherry_blossom'",
            "default_last": "",
        }
        _add_dummy_default(args, key)
        APP_STATE.st_widget(**args)

    # snapshot version
    with col3:
        key = "snapshot_version"
        args = {
            "st_widget": st.text_input,
            "key": key,
            "label": create_display_name_init_section("version"),
            "help": "## Description\n\nVersion of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
            "placeholder": f"Example: '{utils.DATE_TODAY}'",
            "default_last": utils.DATE_TODAY,
        }
        _add_dummy_default(args, key)

        APP_STATE.st_widget(**args)

    cols = st.columns([2, 3])
    with cols[0]:
        # with st_horizontal(vertical_alignment="flex-end"):
        key = "file_extension"
        args = {
            "st_widget": st.text_input,
            "label": create_display_name_init_section("file extension"),
            "key": key,
            "help": "## Description\n\nFile extension (without the '.') of the file to be downloaded.",
            "placeholder": "'csv', 'xls', 'zip'",
            "default_last": "",
        }
        _add_dummy_default(args, key)

        APP_STATE.st_widget(**args)

    with cols[1]:
        # Private dataset?
        APP_STATE.st_widget(
            st.toggle,
            label="Make dataset private",
            help="Check if you want to make the dataset private.",
            key="is_private",
            default_last=False,
        )

        # Import local file?
        APP_STATE.st_widget(
            st.toggle,
            label="Import dataset from local file",
            help="Check if you want to import the snapshot from a local file.",
            key="local_import",
            default_last=False,
        )


def render_fields_from_schema(
    schema: Dict[str, Any],
    property_name: str,
    categories: Optional[List[Any]] = None,
    container: Optional[Any] = None,
):
    """Render fields from schema.

    This function is quite complex, and includes some recursion. Could probably be improved, but it is OK for now.
    """
    # Define containers (if applicable)
    containers = {}
    if categories:
        for cat in categories:
            containers[cat] = st.container()
            containers[cat].markdown(f"#### {cat.capitalize()}")
    # Iterate over schema
    for name, props in schema.items():
        prop_uri = f"{property_name}.{name}"
        # Go deeper
        if "properties" in props:
            if categories:
                render_fields_from_schema(
                    props["properties"],
                    prop_uri,
                    container=containers[cat],  # type: ignore
                )
            else:
                render_fields_from_schema(props["properties"], prop_uri)
        # Render
        else:
            ## Default arguments
            kwargs = {
                # Define display, help and placeholder texts
                "label": create_display_name_snap_section(props, name, property_name),
                "help": create_description(field=props),
                "key": prop_uri,
            }

            ## Text area
            if prop_uri in FIELD_TYPES_TEXTAREA:
                kwargs = {
                    "st_widget": st.text_area,
                    **kwargs,
                    "placeholder": props["examples"][:3] if isinstance(props["examples"], str) else "",
                }

            ## Select box (with custom values option)
            elif prop_uri in FIELD_TYPES_SELECTBOX:
                if prop_uri == "origin.attribution":
                    options = [
                        "{producer} ({year})",
                        "{producer} - {title} {version_producer} ({year})",
                        # "{title} {version_producer} - {producer} ({year})",
                    ]
                else:
                    options = sorted(props["options"])
                kwargs = {
                    "st_widget": st.selectbox,
                    **kwargs,
                    "options": options,
                    "default_last": options[0],
                    "accept_new_options": True,
                }

            ## Text input
            else:
                kwargs = {
                    "st_widget": st.text_input,
                    **kwargs,
                    "placeholder": "Examples: " + ", ".join([f"'{ex}'" for ex in props["examples"]])
                    if props["examples"]
                    else "",
                }

            ## Dummy data
            if prop_uri not in FIELD_TYPES_SELECTBOX:
                _add_dummy_default(kwargs, prop_uri, default_value="")

            # Render field in corresponding container
            if categories:
                with containers[props["category"]]:
                    APP_STATE.st_widget(**kwargs)  # type: ignore
            elif container:
                with container:
                    APP_STATE.st_widget(**kwargs)  # type: ignore
            else:
                APP_STATE.st_widget(**kwargs)  # type: ignore


def update_state() -> None:
    """Submit form."""
    # Create form
    form = SnapshotForm.from_state()
    # Update states with values from form
    APP_STATE.update_from_form(form)


def run_snap_step() -> None:
    """Update state variables to correctly render the UI."""
    st.session_state["show_form"] = False
    st.session_state["run_step"] = True


#########################################################
# MAIN ##################################################
#########################################################

# TITLE
st.title(":material/photo_camera: Snapshot **:gray[Create step]**")

# SIDEBAR
with st.sidebar:
    # INSTRUCTIONS
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()
        st.markdown(text)
        st.markdown("3. **Only supports `origin`**. `source` is not supported anymore.")

# FORM
if st.session_state["show_form"]:
    form_widget = st.empty()
    with form_widget.form("form"):
        # 1) Show fields for initial configuration (create directories, etc.)
        # st.header("Config")
        st.markdown(
            ":small[:gray[Some fields might not be available sometimes (even if they are labelled as required)]]"
        )
        render_fields_init()

        # 2) Show fields for metadata fields
        categories_in_schema = {v["category"] for k, v in SCHEMA_ORIGIN.items()}
        assert categories_in_schema == set(
            ACCEPTED_CATEGORIES
        ), f"Unknown categories in schema: {categories_in_schema - set(ACCEPTED_CATEGORIES)}"

        render_fields_from_schema(SCHEMA_ORIGIN, "origin", categories=ACCEPTED_CATEGORIES)

        # 3) Submit
        submitted = st.form_submit_button("Submit", type="primary", use_container_width=True, on_click=update_state)

else:
    submitted = False
    form_widget = st.empty()

#########################################################
# SUBMISSION ############################################
#########################################################
if submitted:
    # Create form
    form = SnapshotForm.from_state()

    if not form.errors:
        form_widget.empty()

        # Create files
        generate_step(
            cookiecutter_path=COOKIE_SNAPSHOT,
            data=dict(**form.model_dump(), channel="snapshots"),
            target_dir=SNAPSHOTS_DIR,
        )
        ingest_path = SNAPSHOTS_DIR / form.namespace / form.snapshot_version / (form.short_name + ".py")
        meta_path = (
            SNAPSHOTS_DIR / form.namespace / form.snapshot_version / f"{form.short_name}.{form.file_extension}.dvc"
        )

        # Preview generated
        st.subheader("Generated files")
        preview_file(ingest_path, "python")
        preview_file(meta_path, "yaml")

        # Display next steps
        if form.dataset_manual_import:
            manual_import_instructions = "-f **relative path of file**"
        else:
            manual_import_instructions = ""
        st.subheader("Next steps")
        with st.expander("⏭️ **Next steps**", expanded=True):
            # 1/ Verification
            st.markdown("#### 1. Verification")
            st.markdown("Verify that generated files are correct and update them if necessary.")
            # 2/ Run snapshot step
            st.markdown("#### 2. Run snapshot step")
            if form.dataset_manual_import:
                s_file = st.text_input(
                    label="Select local file to import", placeholder="path/to/file.csv", key="snapshot_file"
                )
            st.button("Run snapshot step", key="run_snapshot_step", on_click=run_snap_step)  # type: ignore
            st.markdown(
                f"""
            You can also run the step from the command line:
            ```bash
            python {ingest_path.relative_to(BASE_DIR)} {manual_import_instructions}
            ```
            """
            )

            st.markdown("#### 3. Proceed to next step")
            st_wizard_page_link("data", use_container_width=True, border=True)

        # User message
        st.toast("Templates generated. Read the next steps.", icon="✅")

        # Update config
        utils.update_wizard_defaults_from_form(form=form)

        # st.write(st.session_state)
    else:
        st.write(form.errors)
        st.error("Form not submitted! Check errors!")


#########################################################
# EXECUTION #############################################
#########################################################
if st.session_state["run_step"]:
    # Get form
    form = cast(SnapshotForm, SnapshotForm.from_state())
    # Get snapshot script path
    script_path = f"{SNAPSHOTS_DIR}/{form.namespace}/{form.snapshot_version}/{form.short_name}.py"

    # Build command
    commands = ["uv", "run", "python", script_path]
    if form.dataset_manual_import:
        # Get snapshot local file
        commands.extend(["-f", st.session_state["snapshot_file"]])
    command_str = f"`{' '.join(commands)}`"

    # Run step
    with st.spinner(f"Running snapshot step... {command_str}", show_time=True):
        try:
            output = subprocess.check_output(args=commands)
        except Exception as e:
            st.write("The snapshot was NOT uploaded! Please check the terminal for the complete error message.")
            tb_str = "".join(traceback.format_exception(e))
            st.error(tb_str)
        else:
            st.write("Snapshot should have been uploaded!")

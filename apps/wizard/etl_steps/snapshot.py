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
from apps.wizard.utils.components import preview_file, st_horizontal, st_wizard_page_link
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
FIELD_TYPES_SELECT = ["origin.license.name"]
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


def render_fields_init() -> List[str]:
    """Render fields to create directories and all."""
    form = []

    col1, col2, col3 = st.columns([2, 2, 1], vertical_alignment="bottom")

    # namespace
    with col1:
        field = ["namespace", st.empty(), st.container()]
        form.append(field)
    # short name
    with col2:
        key = "short_name"
        args = {
            "st_widget": st.text_input,
            "label": create_display_name_init_section("short name"),
            "help": "## Description\n\nDataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
            "placeholder": "Example: 'cherry_blossom'",
            "key": key,
            "default_last": "",
        }
        if APP_STATE.args.dummy_data:
            args["value"] = dummy_values[key]

        field = APP_STATE.st_widget(**args)
        form.append(field)
    # snapshot version
    with col3:
        key = "snapshot_version"
        args = {
            "st_widget": st.text_input,
            "label": create_display_name_init_section("version"),
            "help": "## Description\n\nVersion of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
            "placeholder": f"Example: '{utils.DATE_TODAY}'",
            "key": key,
            "default_last": utils.DATE_TODAY,
        }
        if APP_STATE.args.dummy_data:
            args["value"] = dummy_values[key]

        field = APP_STATE.st_widget(**args)
        form.append(field)

    with st_horizontal(vertical_alignment="flex-end"):
        key = "file_extension"
        args = {
            "st_widget": st.text_input,
            "label": create_display_name_init_section("file extension"),
            "help": "## Description\n\nFile extension (without the '.') of the file to be downloaded.",
            "placeholder": "'csv', 'xls', 'zip'",
            "key": key,
            "default_last": "",
        }
        if APP_STATE.args.dummy_data:
            args["value"] = dummy_values[key]

        field = APP_STATE.st_widget(**args)
        form.append(field)

    # Private dataset?
    field = APP_STATE.st_widget(
        st.toggle,
        label="Make dataset private",
        help="Check if you want to make the dataset private.",
        key="is_private",
        default_last=False,
    )
    form.append(field)
    # Import local file?
    field = APP_STATE.st_widget(
        st.toggle,
        label="Import dataset from local file",
        help="Check if you want to import the snapshot from a local file.",
        key="local_import",
        default_last=False,
    )
    form.append(field)

    return form


def render_fields_from_schema(
    schema: Dict[str, Any],
    property_name: str,
    form_fields: List[str],
    categories: Optional[List[Any]] = None,
    container: Optional[Any] = None,
) -> List[str]:
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
        if "properties" in props:
            if categories:
                form_fields = render_fields_from_schema(
                    props["properties"],
                    prop_uri,
                    form_fields,
                    container=containers[cat],  # type: ignore
                )
            else:
                form_fields = render_fields_from_schema(props["properties"], prop_uri, form_fields)
        else:
            # Define display, help and placeholder texts
            display_name = create_display_name_snap_section(props, name, property_name)
            # Render field
            ## Text areas
            if prop_uri in FIELD_TYPES_TEXTAREA:
                # Use text area for these fields
                kwargs = {
                    "label": display_name,
                    "help": create_description(field=props),
                    "placeholder": props["examples"][:3] if isinstance(props["examples"], str) else "",
                    "key": prop_uri,
                }
                if APP_STATE.args.dummy_data:
                    kwargs["value"] = dummy_values.get(prop_uri, "")

                if categories:
                    with containers[props["category"]]:
                        field = APP_STATE.st_widget(st_widget=st.text_area, **kwargs)  # type: ignore
                elif container:
                    with container:
                        field = APP_STATE.st_widget(st_widget=st.text_area, **kwargs)
                else:
                    field = APP_STATE.st_widget(st_widget=st.text_area, **kwargs)
            ## Special case: license name (select box)
            elif prop_uri in ["origin.license.name", "origin.attribution"]:
                # Special one, need to have responsive behaviour inside form (work around)
                if categories:
                    with containers[props["category"]]:
                        field = [prop_uri, st.empty(), st.container()]  # type: ignore
                elif container:
                    with container:
                        field = [prop_uri, st.empty(), st.container()]
                else:
                    field = [prop_uri, st.empty(), st.container()]
            ## Text input
            else:
                # default_value = DEFAULT_VALUES.get(prop_uri, "")
                # Simple text input for the rest
                kwargs = {
                    "label": display_name,
                    "help": create_description(field=props),
                    "placeholder": "Examples: " + ", ".join([f"'{ex}'" for ex in props["examples"]])
                    if props["examples"]
                    else "",
                    "key": prop_uri,
                }
                if APP_STATE.args.dummy_data:
                    kwargs["value"] = dummy_values.get(prop_uri, "")

                if categories:
                    with containers[props["category"]]:
                        field = APP_STATE.st_widget(st.text_input, **kwargs)  # type: ignore
                elif container:
                    with container:
                        field = APP_STATE.st_widget(st.text_input, **kwargs)  # type: ignore
                else:
                    field = APP_STATE.st_widget(st.text_input, **kwargs)  # type: ignore
            # Add field to list
            form_fields.append(cast(str, field))
    return form_fields


def render_license_field(form: List[Any]) -> List[str]:
    """Render the license field within the form.

    We want the license field to be a selectbox, but with the option to add a custom license.

    This is a workaround to have repsonsive behaviour within a form.

    Source: https://discuss.streamlit.io/t/can-i-add-to-a-selectbox-an-other-option-where-the-user-can-add-his-own-answer/28525/5
    """
    # Assert there is only one element of type list
    assert (
        len([field for field in form if isinstance(field, list)]) == 2
    ), "More than one element in the form is of type list!"

    # Get relevant values from schema
    property_name = "origin.license"
    name = "name"
    prop_uri = f"{property_name}.{name}"
    props_license = SCHEMA_ORIGIN["license"]
    props = props_license["properties"][name]
    display_name = create_display_name_snap_section(props, name, property_name)

    # Main decription
    toc = "[Description](#description) "
    help_text = (
        "## Description\n\n" + props["description"] + "\n\n" + props_license["description"].replace("\n", "\n\n\n")
    )
    # Guidelines
    if props.get("guidelines"):
        help_text += "\n## Guidelines" + guidelines_to_markdown(guidelines=props["guidelines"])
        toc += "| [Guidelines](#guidelines) "
    # Examples (good vs bad)
    if props.get("examples"):
        if "examples_bad" in props:
            help_text += "\n## Examples" + examples_to_markdown(
                examples=props["examples"],
                examples_bad=props["examples_bad"],
                extra_tab=0,
                do_sign="✅",
                dont_sign="❌",
            )
        else:
            help_text += "\n## Examples" + examples_to_markdown(
                examples=props["examples"], examples_bad=[], extra_tab=0, do_sign="✅", dont_sign="❌"
            )
        toc += "| [Examples](#examples) "
    help_text = toc.strip() + "\n\n" + help_text
    options = sorted(props["options"])

    # Default option in select box for custom license
    CUSTOM_OPTION = "Custom license..."
    # Render and get element depending on selection in selectbox
    for field in form:
        if isinstance(field, list) and field[0] == "origin.license.name":
            with field[1]:
                license_field = APP_STATE.st_widget(
                    st.selectbox,
                    label=display_name,
                    options=[CUSTOM_OPTION] + options,
                    help=help_text,
                    key=prop_uri,
                    default_last=options[0],
                )
            with field[2]:
                if license_field == CUSTOM_OPTION:
                    license_field = APP_STATE.st_widget(
                        st.text_input,
                        label="↳ *Use custom license*",
                        placeholder="© GISAID 2023",
                        help="Enter custom license",
                        key=f"{prop_uri}_custom",
                    )

    # Remove list from form (former license st.empty tuple)
    form = [f for f in form if not isinstance(f, list)]

    # Add license field
    form.append(license_field)  # type: ignore

    return form


def render_attribution_field(form: List[Any]) -> List[str]:
    """Render the attribution field within the form.

    We want the attribution field to be a selectbox, but with the option to add a custom attribution.

    This is a workaround to have repsonsive behaviour within a form.

    Source: https://discuss.streamlit.io/t/can-i-add-to-a-selectbox-an-other-option-where-the-user-can-add-his-own-answer/28525/5
    """
    # Assert there is only one element of type list
    assert (
        len([field for field in form if isinstance(field, list)]) == 2
    ), "More than one element in the form is of type list!"

    # Get relevant values from schema
    parent = "origin"
    name = "attribution"
    prop_uri = f"{parent}.{name}"
    props = SCHEMA_ORIGIN[name]
    display_name = create_display_name_snap_section(props, name, parent, title="Attribution format")

    # Main decription
    toc = "[Description](#description) "
    description_add = """Use this dropdown to select the desired `attribution` format.

The default option should be used in most cases. Alternatively other common format options are available.

Only in rare occasions you will need to define a custom attribution.
    """
    help_text = "## Description\n\n" + description_add
    # Guidelines
    if props.get("guidelines"):
        help_text += "\n## Guidelines" + guidelines_to_markdown(guidelines=props["guidelines"])
        toc += "| [Guidelines](#guidelines) "
    # Examples (good vs bad)
    if props.get("examples"):
        if "examples_bad" in props:
            help_text += "\n## Examples" + examples_to_markdown(
                examples=props["examples"],
                examples_bad=props["examples_bad"],
                extra_tab=0,
                do_sign="✅",
                dont_sign="❌",
            )
        else:
            help_text += "\n## Examples" + examples_to_markdown(
                examples=props["examples"], examples_bad=[], extra_tab=0, do_sign="✅", dont_sign="❌"
            )
        toc += "| [Examples](#examples) "
    help_text = toc.strip() + "\n\n" + help_text

    # Options
    DEFAULT_OPTION = "{producer} ({year})"
    options = [
        DEFAULT_OPTION,
        "{producer} - {title} {version_producer} ({year})",
        # "{title} {version_producer} - {producer} ({year})",
    ]

    # Default option in select box for custom license
    CUSTOM_OPTION = "Custom attribution..."
    # Render and get element depending on selection in selectbox
    for field in form:
        if isinstance(field, list) and field[0] == "origin.attribution":
            with field[1]:
                attribution_field = APP_STATE.st_widget(
                    st.selectbox,
                    label=display_name,
                    options=["Custom attribution..."] + options,
                    help=help_text,
                    key=prop_uri,
                    default_last=options[0],
                )
            with field[2]:
                if attribution_field == CUSTOM_OPTION:
                    attribution_field = APP_STATE.st_widget(
                        st.text_input,
                        label="↳ *Use custom attribution*",
                        placeholder="",
                        help="Enter custom attribution. Make sure to add the explicit attribution and not its format (as in the dropdown options)!",
                        key=f"{prop_uri}_custom",
                    )

    # Remove list from form (former license st.empty tuple)
    form = [f for f in form if not isinstance(f, list)]

    # Add license field
    form.append(attribution_field)  # type: ignore

    return form


def render_namespace_field(form: List[Any]) -> List[str]:
    """Render the namespace field within the form.

    We want the namespace field to be a selectbox, but with the option to add a custom namespace.

    This is a workaround to have repsonsive behaviour within a form.

    Source: https://discuss.streamlit.io/t/can-i-add-to-a-selectbox-an-other-option-where-the-user-can-add-his-own-answer/28525/5
    """
    # Assert there is only one element of type list
    assert (
        len([field for field in form if isinstance(field, list)]) == 1
    ), "Only one element in the form is allowed to be of type list!"

    # Get relevant values from schema
    prop_uri = "namespace"
    display_name = create_display_name_init_section("Namespace")

    # Main decription
    help_text = "## Description\n\nInstitution or topic name"

    # Options
    DEFAULT_OPTION = OPTIONS_NAMESPACES[0]

    # Default option in select box for custom license
    CUSTOM_OPTION = "Custom namespace..."
    # Render and get element depending on selection in selectbox
    for field in form:
        if isinstance(field, list) and field[0] == "namespace":
            with field[1]:
                if APP_STATE.args.dummy_data:
                    namespace_field = APP_STATE.st_widget(
                        st.selectbox,
                        label=display_name,
                        options=[CUSTOM_OPTION] + OPTIONS_NAMESPACES,
                        help=help_text,
                        key=prop_uri,
                        default_last=dummy_values[prop_uri],
                    )
                else:
                    namespace_field = APP_STATE.st_widget(
                        st.selectbox,
                        label=display_name,
                        options=[CUSTOM_OPTION] + OPTIONS_NAMESPACES,
                        help=help_text,
                        key=prop_uri,
                        default_last=DEFAULT_OPTION,
                    )
            with field[2]:
                if namespace_field == CUSTOM_OPTION:
                    namespace_field = APP_STATE.st_widget(
                        st.text_input,
                        label="↳ *Use custom namespace*",
                        placeholder="",
                        help="Enter custom namespace.",
                        key=f"{prop_uri}_custom",
                    )

    # Remove list from form (former license st.empty tuple)
    form = [f for f in form if not isinstance(f, list)]

    # Add license field
    form.append(namespace_field)  # type: ignore

    return form


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
        st.markdown("Note that sometimes some fields might not be available (even if they are labelled as required)")
        form_init = render_fields_init()

        # 2) Show fields for metadata fields
        # st.header("Metadata")
        # st.markdown(
        #     "Fill the following fields to help us fill all the created files for you! Note that sometimes some fields might not be available (even if they are labelled as required)."
        # )
        # Get categories
        for k, v in SCHEMA_ORIGIN.items():
            if "category" not in v:
                print(k)
                print(v)
                # raise ValueError(f"Category not found for {k}")
        categories_in_schema = {v["category"] for k, v in SCHEMA_ORIGIN.items()}
        assert categories_in_schema == set(
            ACCEPTED_CATEGORIES
        ), f"Unknown categories in schema: {categories_in_schema - set(ACCEPTED_CATEGORIES)}"

        form_metadata = render_fields_from_schema(SCHEMA_ORIGIN, "origin", [], categories=ACCEPTED_CATEGORIES)

        # 3) Submit
        submitted = st.form_submit_button("Submit", type="primary", use_container_width=True, on_click=update_state)

    # 2.1) Create fields for namespace (responsive within form)
    form = render_namespace_field(form_init)

    # 2.1) Create fields for attribution (responsive within form)
    form = render_attribution_field(form_metadata)

    # 2.1) Create fields for License (responsive within form)
    form = render_license_field(form_metadata)
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
            args = []
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
            # st.markdown("Or use **Express** mode to create a Meadow, Garden and Grapher steps at once.")
            # st_wizard_page_link("express", use_container_width=True, border=True)

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
    command = f"uv run python {script_path}"
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
            st.write("Snapshot should be uploaded!")

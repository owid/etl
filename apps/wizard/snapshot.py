import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, cast

import streamlit as st
from botocore.exceptions import ClientError
from owid.catalog import s3_utils
from pydantic import BaseModel

from apps.wizard import utils
from etl.helpers import read_json_schema
from etl.paths import APPS_DIR, SCHEMAS_DIR, SNAPSHOTS_DIR

#########################################################
# CONSTANTS #############################################
#########################################################
# Read schema
SNAPSHOT_SCHEMA = read_json_schema(SCHEMAS_DIR / "snapshot-schema.json")
# Get properties for origin in schema
schema_origin = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]["properties"]
# Lists with fields of special types. By default, fields are text inputs.
FIELD_TYPES_TEXTAREA = ["origin.dataset_description_owid", "origin.dataset_description_producer"]
FIELD_TYPES_SELECT = ["origin.license.name"]
# Get current directory
CURRENT_DIR = Path(__file__).parent
# Accepted schema categories
ACCEPTED_CATEGORIES = ["dataset", "citation", "files", "license"]
form_fields = []

# Session state variables initialitzatio


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
class SnapshotForm(BaseModel):
    """Interface for snapshot form."""

    # config
    namespace: str
    snapshot_version: str
    short_name: str
    file_extension: str
    is_private: bool
    dataset_manual_import: bool

    # origin
    dataset_title_owid: str
    dataset_description_owid: str
    dataset_title_producer: str
    origin_version: str
    date_published: str
    dataset_description_producer: str
    producer: str
    citation_producer: str
    attribution: str
    attribution_short: str
    dataset_url_main: str
    dataset_url_download: str
    date_accessed: str

    # license
    license_url: str
    license_name: str

    def __init__(self, **data: Any) -> None:
        # Change name for certain fields (and remove old ones)
        data["license_url"] = data["origin.license.url"]
        if "origin.license.name_custom" in data:
            data["license_name"] = data["origin.license.name_custom"]
        else:
            data["license_name"] = data["origin.license.name"]
        data["origin_version"] = data["origin.version"]
        data["is_private"] = data["private"]
        data["dataset_manual_import"] = data["local_import"]
        data = {
            k: v for k, v in data.items() if k not in ["origin.license.url", "origin.license.name", "origin.version"]
        }
        # Remove 'origin.' prefix from keys
        data = {k.replace("origin.", ""): v for k, v in data.items()}
        super().__init__(**data)

    @property
    def version_producer(self):
        return self.origin_version

    @property
    def private(self):
        return self.is_private


def _pretty_req_level(requirement_level: str) -> str:
    """Prettify the requirement level with coloring."""
    if requirement_level == "required":
        return f"_:red[{requirement_level}]_"
    elif requirement_level == "recommended":
        return f"_:orange[{requirement_level}]_"
    elif requirement_level == "optional":
        return f"_:blue[{requirement_level}]_"
    else:
        raise ValueError(f"Unknown requirement level: {requirement_level}")


def _color_req_level(req_level: str) -> str:
    if req_level == "required":
        # color = "red"
        return f"**:red[{req_level}]**"
    elif req_level == "recommended":
        color = "orange"
    elif req_level == "optional":
        color = "blue"
    else:
        raise ValueError(f"Unknown requirement level: {req_level}")
    req_level = f":{color}[{req_level}]"
    return req_level


def create_display_name_init_section(name: str) -> str:
    """Create display name for a field."""
    # Get requirement level colored
    req_level = _color_req_level("required")
    # Create display name
    display_name = f"{name} ┃ {req_level}"
    return display_name


def create_display_name_snap_section(props: Dict[str, Any], name: str, property_name: str) -> str:
    """Create display name for a field."""
    # Get requirement level colored
    req_level = _color_req_level(props["requirement_level"])
    # Create display name
    display_name = f"{props['title']} (`{property_name}.{name}`) ┃ {req_level}"
    return display_name


def render_fields_init() -> List[Any]:
    """Render fields to create directories and all."""
    form = []
    # Text inputs
    fields = [
        ("Namespace", "Institution or topic name", "Example: emdat, health"),
        (
            "Snapshot Version",
            "Version of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
            f"Example: {dt.date.today()}",
        ),
        ("Short name", "Underscored dataset short name. Example: natural_disasters", "Example: cherry_blossom"),
        ("File extension", "File extension (without the '.') of the file to be downloaded.", "Example: csv, xls, zip"),
    ]
    for field in fields:
        field = st.text_input(
            create_display_name_init_section(field[0]),
            help=field[1],
            placeholder=field[2],
            key=field[0].replace(" ", "_").lower(),
        )
        form.append(field)

    form.extend(
        [
            st.checkbox(
                label="Make dataset private",
                value=False,
                key="private",
                help="Check if you want to make the dataset private.",
            ),
            st.checkbox(
                label="Import dataset from local file",
                value=False,
                key="local_import",
                help="Check if you want to import the snapshot from a local file.",
            ),
        ]
    )
    return form


def render_fields_from_schema(
    schema: Dict[str, Any],
    property_name: str,
    form_fields: List[str],
    categories: Optional[Set] = None,
    container: List[str] = None,
) -> List[str]:
    """Render fields from schema.

    This function is quite complex, and includes some recursion. Could probably be improved, but it is OK for now.
    """
    # Define containers (if applicable)
    if categories:
        containers = {}
        for cat in categories:
            containers[cat] = st.container()
            containers[cat].markdown(f"#### {cat.capitalize()}")
    # Iterate over schema
    for name, props in schema.items():
        prop_uri = f"{property_name}.{name}"
        if "properties" in props:
            if categories:
                form_fields = render_fields_from_schema(
                    props["properties"], prop_uri, form_fields, container=containers[cat]  # type: ignore
                )
            else:
                form_fields = render_fields_from_schema(props["properties"], prop_uri, form_fields)
        else:
            # Define display, help and placeholder texts
            display_name = create_display_name_snap_section(props, name, property_name)
            # Render field
            if prop_uri in FIELD_TYPES_TEXTAREA:
                # Use text area for these fields
                if categories:
                    field = containers[props["category"]].text_area(  # type: ignore
                        display_name, help=props["description"], placeholder="", key=prop_uri
                    )
                elif container:
                    field = container.text_area(display_name, help=props["description"], placeholder="", key=prop_uri)
                else:
                    field = st.text_area(display_name, help=props["description"], placeholder="", key=prop_uri)
            elif prop_uri == "origin.license.name":
                # Special one, need to have responsive behaviour inside form (work around)
                if categories:
                    field = [containers[props["category"]].empty(), containers[props["category"]].empty()]  # type: ignore
                elif container:
                    field = [container.empty(), container.empty()]
                else:
                    field = [st.empty(), st.empty()]
            else:
                # Simple text input for the rest
                if categories:
                    field = containers[props["category"]].text_input(  # type: ignore
                        display_name, help=props["description"], placeholder="", key=prop_uri
                    )
                elif container:
                    field = container.text_input(display_name, help=props["description"], placeholder="", key=prop_uri)
                else:
                    field = st.text_input(display_name, help=props["description"], placeholder="", key=prop_uri)
            # Add field to list
            form_fields.append(cast(str, field))
    return form_fields


@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(CURRENT_DIR / "snapshot.md", "r") as f:
        return f.read()


def render_license_field(form: List[str]) -> List[str]:
    """Render the license field within the form.

    We want the license field to be a selectbox, but with the option to add a custom license.

    This is a workaround to have repsonsive behaviour within a form.

    Source: https://discuss.streamlit.io/t/can-i-add-to-a-selectbox-an-other-option-where-the-user-can-add-his-own-answer/28525/5
    """
    # Assert there is only one element of type list
    assert (
        len([field for field in form if isinstance(field, list)]) == 1
    ), "More than one element in the form is of type list!"

    # Get relevant values from schema
    property_name = "origin.license"
    name = "name"
    prop_uri = f"{property_name}.{name}"
    props_license = schema_origin["license"]
    props = props_license["properties"][name]
    display_name = create_display_name_snap_section(props, name, property_name)

    help_text = props["description"] + "\n\n" + props_license["description"].replace("\n", "\n\n\n")
    options = props["options"]

    # Default option in select box for custom license
    CUSTOM_OPTION = "Custom license..."
    # Render and get element depending on selection in selectbox
    for field in form:
        if isinstance(field, list):
            with field[0]:
                license_field = st.selectbox(
                    display_name, options=["Custom license..."] + sorted(options), index=1, help=help_text, key=prop_uri
                )
            with field[1]:
                if license_field == CUSTOM_OPTION:
                    license_field = st.text_input(
                        "↳ *Custom license*",
                        placeholder="© GISAID 2023",
                        help="Enter custom license",
                        key=f"{prop_uri}_custom",
                    )

    # Remove list from form (former license st.empty tuple)
    form = [f for f in form if not isinstance(f, list)]

    # Add license field
    form.append(license_field)  # type: ignore

    return form


@st.cache_resource
def _aws_is_ok():
    try:
        s3_utils.check_for_default_profile()
    except s3_utils.MissingCredentialsError as e:
        return e


@st.cache_resource
def _get_s3_buckets():
    s3 = s3_utils.connect()
    try:
        buckets = s3.list_buckets()["Buckets"]
    except ClientError as e:
        return (True, e)
    return (False, buckets)


def run_checks() -> None:
    """Environment checks."""
    text_reference_expander = "\n\nExpand the **Environment checks** section for more details!"
    # AWS config
    aws_error = _aws_is_ok()
    if aws_error:
        text = "Invalid AWS profile:\n{}".format(aws_error)
        st.error(text)
        st.toast(text + text_reference_expander, icon="❌")
        raise aws_error
    else:
        text = "AWS profile is valid"
        # st.toast(text, icon="✅")
        st.success(text)

    # S3 conncetion
    error, buckets_or_error = _get_s3_buckets()
    if error:
        text = "Error connecting to S3:\n{}".format(buckets_or_error)
        st.error(text)
        st.toast(text + text_reference_expander, icon="❌")
        raise buckets_or_error
    else:
        text = "S3 connection successful"
        # st.toast(text, icon="✅")
        st.success(text)

        bucket_names = [b["Name"] for b in buckets_or_error]  # type: ignore
        if "owid-catalog" not in bucket_names:
            text = "`owid-catalog` bucket not found"
            st.error(text)
            st.toast(text + text_reference_expander, icon="❌")
            raise Exception()


#########################################################
# MAIN ##################################################
#########################################################

# TITLE
st.title("Wizard: Snapshot")

# INSTRUCTIONS
with st.expander("**Instructions**"):
    text = load_instructions()
    st.markdown(text)
    st.markdown("3. **Only supports `origin`**. To work with `source` instead, use the former version of the app.")

# CONNECT AND CHECK
with st.expander("Environment checks", expanded=True):
    run_checks()

# FORM
with st.form("form"):

    # 1) Show fields for initial configuration (create directories, etc.)
    st.header("Config")
    st.markdown("Fill the following fields to help us create all the required files for you!")
    form_init = render_fields_init()

    # 2) Show fields for metadata fields
    st.header("Metadata")
    st.markdown(
        "Fill the following fields to help us fill all the created files for you! Note that sometimes some fields might not be available (even if they are labelled as required)."
    )

    # Get categories
    categories_in_schema = {v["category"] for k, v in schema_origin.items()}
    assert categories_in_schema == set(
        ACCEPTED_CATEGORIES
    ), f"Unknown categories in schema: {categories_in_schema - set(ACCEPTED_CATEGORIES)}"

    form_metadata = render_fields_from_schema(schema_origin, "origin", [], categories=ACCEPTED_CATEGORIES)

    # 3) Submit
    submitted = st.form_submit_button("Submit", type="primary", use_container_width=True)

# 2.1) Create fields for License (responsive within form)
form = render_license_field(form_metadata)

print(form_init)
print(form_metadata)
#########################################################
# AFTER SUBMISSION ######################################
#########################################################
if submitted:
    # Create form
    form = SnapshotForm(**cast(Dict[str, Any], st.session_state))
    st.write(st.session_state)
    # Generate step
    cookiecutter_path = APPS_DIR / "walkthrough" / "snapshot_origins_cookiecutter/"
    utils.generate_step(
        cookiecutter_path,
        dict(**form.dict(), channel="snapshots", walkthrough_origins=utils.WALKTHROUGH_ORIGINS),
        SNAPSHOTS_DIR,
    )

    ingest_path = SNAPSHOTS_DIR / form.namespace / form.snapshot_version / (form.short_name + ".py")
    meta_path = SNAPSHOTS_DIR / form.namespace / form.snapshot_version / f"{form.short_name}.{form.file_extension}.dvc"

    if form.dataset_manual_import:
        manual_import_instructions = "--path-to-file **relative path of file**"
    else:
        manual_import_instructions = ""

    st.markdown(
        f"""
    ## Next steps

    1. Verify that generated files are correct and update them if necessary

    2. Run the snapshot step to upload files to S3
    ```bash
    python snapshots/{form.namespace}/{form.snapshot_version}/{form.short_name}.py {manual_import_instructions}
    ```

    3. Continue to the meadow step
    """
    )

    # Previews
    st.markdown(
        """
        ## Generated files
        """
    )

    with open(meta_path) as f:
        t = f.read()
    st.success(f"File `{meta_path}` was successfully generated")
    st.code(t, "yaml")

    with open(ingest_path) as f:
        t = f.read()
    st.success(f"File `{ingest_path}` was successfully generated")
    st.code(t, "python")
    # utils.preview_file(meta_path, "yaml")
    # utils.preview_file(ingest_path, "python")

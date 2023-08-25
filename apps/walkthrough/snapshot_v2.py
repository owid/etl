import datetime as dt
from typing import Any, Callable, List

import streamlit as st

from etl.helpers import read_json_schema
from etl.paths import SCHEMAS_DIR

SNAPSHOT_SCHEMA = read_json_schema(SCHEMAS_DIR / "snapshot-schema.json")

schema_origin = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]["properties"]

form_fields = []
FIELD_TYPES_TEXTAREA = ["dataset_description_owid", "dataset_description_producer"]


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


def create_display_name_snap_section(props: str, name: str, property_name: str) -> str:
    """Create display name for a field."""
    # Get requirement level colored
    req_level = _color_req_level(props["requirement_level"])
    # Create display name
    display_name = f"{props['title']} (`{property_name}.{name}`) ┃ {req_level}"
    return display_name


def render_fields_from_schema(schema: List[Any], property_name: str, form_fields: List[Callable]) -> List[Callable]:
    """Render fields from schema."""
    for name, props in schema.items():
        if "properties" in props:
            form_fields = render_fields_from_schema(props["properties"], f"{property_name}.{name}", form_fields)
        else:
            # Define display, help and placeholder texts
            display_name = create_display_name_snap_section(props, name, property_name)
            # Render field
            if name in FIELD_TYPES_TEXTAREA:
                field = st.text_area(display_name, help=props["description"], placeholder="")
            else:
                field = st.text_input(display_name, help=props["description"], placeholder="")
            # Add field to list
            form_fields.append(field)
    return form_fields


def render_fields_init() -> List[Any]:
    """Render fields to create directories and all."""
    form = []
    fields = [
        ("Namespace", "Institution or topic name", "Example: emdat, health"),
        (
            "Version",
            "Version of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
            f"Example: {dt.date.today()}",
        ),
        ("Short name", "Underscored dataset short name. Example: natural_disasters", "Example: cherry_blossom"),
        ("File extension", "File extension (without the '.') of the file to be downloaded.", "Example: csv, xls, zip"),
    ]
    for field in fields:
        field = st.text_input(create_display_name_init_section(field[0]), help=field[1], placeholder=field[2])
        form.append(field)
    pass


st.title("Walkthrough: Snapshot")
with st.form("my_form"):

    st.markdown("Fill the following fields to help us create all the required files for you!")
    form_init = render_fields_init()

    st.header("Snapshot metadata")
    st.markdown(
        "Fill the following fields to help us fill all the created files for you! Note that sometimes some fields might not be available (even if they are labelled as required)."
    )
    form_fields = []
    form = render_fields_from_schema(schema_origin, "origin", form_fields)
    # producer = st.text_input("origins.producer _:red[mandatory]_", help=text_producer, placeholder="e.g. NASA")
    # date_published = st.text_input("origins.date_published _:red[mandatory]_", help=text, placeholder="e.g. 2021-01-01, 2023")
    # dataset_title_producer = st.text_input("origins.dataset_title_producer _:orange[recommended]_", help="Name of the institution or the author(s) that produced the dataset.", placeholder="(mandatory) e.g. NASA")
    # attribution = st.text_input("origins.attribution _:green[optional]_", help="Name of the institution or the author(s) that produced the dataset.", placeholder="(mandatory) e.g. NASA")
    submitted = st.form_submit_button("Submit", type="primary", use_container_width=True)

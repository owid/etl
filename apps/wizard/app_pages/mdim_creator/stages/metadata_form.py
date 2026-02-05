"""Stage 2: Metadata Configuration.

This stage collects metadata for the MDIM:
- Title and title variant
- Topic tags
- Default selection (countries)
- Namespace, short name, version
"""

import re
from typing import Any

import streamlit as st

from apps.wizard.etl_steps.utils import TAGS_DEFAULT

# Priority entities to show at the top of the selection list
PRIORITY_ENTITIES = [
    "World",
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "South America",
    "Oceania",
]


def get_entities_from_table(tb: Any) -> list[str]:
    """Extract unique entity names from the table."""
    # Try to get country/entity from index or columns
    entities: set[str] = set()

    # Check if table has index with country
    if hasattr(tb, "index") and tb.index is not None:
        index_names = list(tb.index.names) if hasattr(tb.index, "names") else []
        if "country" in index_names:
            idx = index_names.index("country")
            if hasattr(tb.index, "levels"):
                # MultiIndex
                entities = set(tb.index.levels[idx].tolist())
            else:
                entities = set(tb.index.tolist())

    # If not found in index, check columns
    if not entities:
        for col in ["country", "entity", "entity_name", "location"]:
            if col in tb.columns:
                entities = set(tb[col].dropna().unique().tolist())
                break

    # Sort with priority entities first
    entity_list = sorted(entities)
    priority = [e for e in PRIORITY_ENTITIES if e in entity_list]
    others = [e for e in entity_list if e not in PRIORITY_ENTITIES]

    return priority + others


def is_snake_case(s: str) -> bool:
    """Check if a string is in snake_case."""
    if not s:
        return False
    return bool(re.fullmatch(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", s))


def is_valid_version(s: str) -> bool:
    """Check if a string is a valid version (YYYY-MM-DD, YYYY, or 'latest')."""
    if not s:
        return False
    return bool(re.fullmatch(r"^\d{4}-\d{2}-\d{2}$|^\d{4}$|^latest$", s))


def render() -> None:
    """Render the metadata form stage."""
    st.subheader(":material/edit_note: Configure Metadata")

    st.markdown(
        """
        Configure the metadata for your MDIM. This includes the title, tags,
        and other settings that will appear in the collection.
        """
    )

    # Initialize from session state
    metadata = st.session_state.get("mdim_metadata", {})
    errors: dict[str, str] = {}

    # Row 1: Title fields
    col1, col2 = st.columns(2)
    with col1:
        title = st.text_input(
            "Title",
            value=metadata.get("title", ""),
            help="Main title for the MDIM",
            placeholder="e.g., Vaccination coverage",
        )
        if not title:
            errors["title"] = "Title is required"
    with col2:
        title_variant = st.text_input(
            "Title variant (optional)",
            value=metadata.get("title_variant", ""),
            help="Optional subtitle",
            placeholder="e.g., by vaccine and metric",
        )

    # Row 2: Tags and default selection
    col1, col2 = st.columns(2)
    with col1:
        topic_tags = st.multiselect(
            "Topic tags",
            options=TAGS_DEFAULT,
            default=metadata.get("topic_tags", []),
            help="Select topic tags for categorization",
        )
        if not topic_tags:
            topic_tags = ["Uncategorized"]
        if len(topic_tags) > 1 and "Uncategorized" in topic_tags:
            errors["topic_tags"] = "Cannot combine 'Uncategorized' with other tags"
            st.error(errors["topic_tags"])

    with col2:
        # Get entities from the loaded table
        tb = st.session_state.get("mdim_table")
        available_entities = get_entities_from_table(tb) if tb is not None else PRIORITY_ENTITIES

        # Filter saved defaults to only include valid entities
        saved_defaults = metadata.get("default_selection", ["World"])
        valid_defaults = [e for e in saved_defaults if e in available_entities]
        if not valid_defaults and "World" in available_entities:
            valid_defaults = ["World"]

        default_selection = st.multiselect(
            "Default selection",
            options=available_entities,
            default=valid_defaults,
            help="Countries/regions shown by default",
        )
        if not default_selection and available_entities:
            default_selection = [available_entities[0]]

    # Row 3: Identifiers
    col1, col2, col3 = st.columns(3)
    with col1:
        namespace = st.text_input(
            "Namespace",
            value=metadata.get("namespace", ""),
            help="snake_case namespace",
            placeholder="e.g., health",
        )
        if not namespace:
            errors["namespace"] = "Namespace is required"
        elif not is_snake_case(namespace):
            errors["namespace"] = "Must be snake_case"
            st.error(errors["namespace"])

    with col2:
        short_name = st.text_input(
            "Short name",
            value=metadata.get("short_name", ""),
            help="snake_case short name",
            placeholder="e.g., vaccination_coverage",
        )
        if not short_name:
            errors["short_name"] = "Short name is required"
        elif not is_snake_case(short_name):
            errors["short_name"] = "Must be snake_case"
            st.error(errors["short_name"])

    with col3:
        version = st.text_input(
            "Version",
            value=metadata.get("version", "latest"),
            help="YYYY-MM-DD, YYYY, or 'latest'",
            placeholder="latest",
        )
        if not version:
            errors["version"] = "Version is required"
        elif not is_valid_version(version):
            errors["version"] = "Invalid format"
            st.error(errors["version"])

    # Output URI preview
    if namespace and short_name and version:
        st.caption(f"Step URI: `export://multidim/{namespace}/{version}/{short_name}`")

    # Update session state
    st.session_state.mdim_metadata = {
        "title": title,
        "title_variant": title_variant,
        "topic_tags": topic_tags,
        "default_selection": default_selection,
        "namespace": namespace,
        "short_name": short_name,
        "version": version,
    }

    # Check if complete (all required fields filled and valid)
    required_fields = ["title", "namespace", "short_name", "version"]
    is_complete = all(st.session_state.mdim_metadata.get(f) for f in required_fields) and not errors

    st.session_state.mdim_metadata_complete = is_complete

    if errors:
        st.warning("Please fix the errors above before continuing.")
    elif is_complete:
        st.success("Metadata configuration complete. Continue to the next step.")

"""Stage 1: Data Source Selection.

This stage allows users to select a grapher step from the DAG or paste a step URI.
The dataset is loaded (or run if not yet executed) and a preview is shown.
"""

import subprocess
from typing import Any

import pandas as pd
import streamlit as st
from owid.catalog import Dataset
from structlog import get_logger

from etl.dag_helpers import load_dag
from etl.paths import DATA_DIR

log = get_logger()


@st.cache_data(ttl=3600)
def get_grapher_steps() -> list[str]:
    """Get all grapher steps from the local DAG."""
    dag = load_dag()
    grapher_steps = sorted([step for step in dag.keys() if step.startswith("data://grapher/")])
    return grapher_steps


def parse_step_uri(step_uri: str) -> dict[str, str]:
    """Parse step URI into components.

    Args:
        step_uri: URI like "data://grapher/energy/2024-01-01/energy_prices"

    Returns:
        Dict with channel, namespace, version, short_name
    """
    # Handle various formats
    step_uri = step_uri.strip()

    # Remove data:// prefix if present
    if step_uri.startswith("data://"):
        step_uri = step_uri[7:]
    elif step_uri.startswith("data-private://"):
        step_uri = step_uri[15:]

    parts = step_uri.split("/")
    if len(parts) < 4:
        raise ValueError(f"Invalid step URI format: {step_uri}")

    return {
        "channel": parts[0],  # grapher
        "namespace": parts[1],
        "version": parts[2],
        "short_name": parts[3],
    }


def get_dataset_path(step_uri: str) -> str:
    """Get the local path for a dataset."""
    info = parse_step_uri(step_uri)
    return str(DATA_DIR / info["channel"] / info["namespace"] / info["version"] / info["short_name"])


def load_dataset(step_uri: str) -> Dataset | None:
    """Load a dataset from the local catalog."""
    try:
        path = get_dataset_path(step_uri)
        return Dataset(path)
    except FileNotFoundError:
        return None


def run_step(step_uri: str) -> tuple[bool, str]:
    """Run an ETL step.

    Returns:
        Tuple of (success, output_message)
    """
    result = subprocess.run(
        [".venv/bin/etlr", step_uri, "--private"],
        capture_output=True,
        text=True,
        cwd="/home/lucas/repos/etl",
    )
    if result.returncode != 0:
        return False, f"Error: {result.stderr}"
    return True, result.stdout


def get_table_info(ds: Dataset) -> dict[str, Any]:
    """Get information about the tables in a dataset."""
    info: dict[str, Any] = {
        "table_names": ds.table_names,
        "tables": {},
    }

    for table_name in ds.table_names:
        tb = ds[table_name]
        # Get column info
        columns: list[dict[str, Any]] = []
        for col in tb.columns:
            col_info: dict[str, Any] = {
                "name": col,
                "dtype": str(tb[col].dtype),
            }
            # Check for dimension metadata
            if hasattr(tb[col], "m") and hasattr(tb[col].m, "dimensions"):
                col_info["dimensions"] = tb[col].m.dimensions
            columns.append(col_info)

        info["tables"][table_name] = {
            "columns": columns,
            "n_columns": len(columns),
            "shape": tb.shape if hasattr(tb, "shape") else None,
        }

    return info


def reset_downstream_state() -> None:
    """Reset all state downstream of dataset selection."""
    st.session_state.mdim_metadata = {}
    st.session_state.mdim_dimension_mapping = []
    st.session_state.mdim_common_config = {}
    st.session_state.mdim_table = None
    st.session_state.mdim_columns = []
    # Reset completion flags
    st.session_state.mdim_dataset_loaded = False
    st.session_state.mdim_metadata_complete = False
    st.session_state.mdim_dimensions_complete = False
    st.session_state.mdim_generated = False


def render() -> None:
    """Render the dataset picker stage."""
    st.subheader(":material/database: Select Data Source")

    st.markdown(
        """
        Select a grapher step from the dropdown or paste a step URI directly.
        The dataset will be loaded and you'll see a preview of its structure.
        """
    )

    # Get available steps
    grapher_steps = get_grapher_steps()

    # Determine default index
    current_step = st.session_state.mdim_selected_step
    if current_step and current_step in grapher_steps:
        default_index = grapher_steps.index(current_step)
    else:
        default_index = None

    # Single selectbox that accepts custom input
    step_uri = st.selectbox(
        "Select grapher step",
        options=grapher_steps,
        index=default_index,
        placeholder="Select or paste a step URI (e.g., data://grapher/...)",
        help="Choose from the list or type/paste a custom step URI",
        accept_new_options=True,
        key="mdim_step_dropdown",
    )

    if step_uri:
        # Check if dataset selection changed - reset downstream state
        previous_step = st.session_state.get("mdim_selected_step")
        if previous_step and previous_step != step_uri:
            reset_downstream_state()
            st.toast("Dataset changed - previous configuration cleared.", icon=":material/refresh:")

        st.session_state.mdim_selected_step = step_uri

        # Try to load the dataset
        with st.spinner("Loading dataset..."):
            ds = load_dataset(step_uri)

        if ds is None:
            st.warning("Dataset not found locally. Step may need to be run first.")

            if st.button("Run step now", type="primary"):
                with st.spinner(f"Running step `{step_uri}`...", show_time=True):
                    success, message = run_step(step_uri)

                if success:
                    st.success("Step completed successfully!")
                    ds = load_dataset(step_uri)
                else:
                    st.error(f"Step failed: {message}")

        if ds is not None:
            st.success(f"Dataset loaded: **{ds.metadata.short_name}**")

            # Get table info
            table_info = get_table_info(ds)

            # Show table selector if multiple tables
            if len(table_info["table_names"]) > 1:
                selected_table = st.selectbox(
                    "Select table",
                    options=table_info["table_names"],
                    help="This dataset has multiple tables. Select one to use.",
                )
            else:
                selected_table = table_info["table_names"][0]

            # Show table info
            with st.expander(f"Table: `{selected_table}`", expanded=True):
                tb = ds[selected_table]
                cols_info = table_info["tables"][selected_table]["columns"]

                st.markdown(f"**Shape:** {tb.shape[0]:,} rows x {tb.shape[1]} columns")

                # Create column summary DataFrame
                col_df = pd.DataFrame(cols_info)
                col_df["has_dimensions"] = col_df.apply(
                    lambda x: "dimensions" in x and x["dimensions"] is not None,
                    axis=1,
                )
                st.dataframe(col_df, use_container_width=True, hide_index=True)

                # Show sample data
                st.markdown("**Sample data (first 5 rows):**")
                sample_df = tb.head(5).reset_index()
                st.dataframe(sample_df, use_container_width=True)

            # Store in session state
            st.session_state.mdim_table = tb
            st.session_state.mdim_columns = [c["name"] for c in cols_info]
            st.session_state.mdim_dataset_loaded = True

            # Extract default metadata from dataset
            if not st.session_state.mdim_metadata:
                try:
                    info = parse_step_uri(step_uri)
                    st.session_state.mdim_metadata = {
                        "namespace": info["namespace"],
                        "short_name": info["short_name"],
                        "version": info["version"],
                        "title": ds.metadata.title or "",
                        "title_variant": "",
                        "topic_tags": list(ds.metadata.licenses) if hasattr(ds.metadata, "licenses") else [],
                        "default_selection": ["World"],
                    }
                except Exception:
                    pass

    else:
        st.info("Select a grapher step or paste a URI to continue.")
        st.session_state.mdim_dataset_loaded = False

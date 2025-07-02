"""
Dataset to Google Sheet Exporter

Allows users to select a dataset and table, then creates a public read-only Google Sheet.
"""

from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from apps.utils.google import GoogleDrive, GoogleSheet
from apps.wizard.utils import cached

# Page config
st.set_page_config(page_title="Dataset to Google Sheet", page_icon="📊", layout="wide")

st.title("📊 Dataset to Google Sheet Exporter")
st.markdown("Select a dataset and table to create a public read-only Google Sheet.")

# Initialize session state
if "sheet_created" not in st.session_state:
    st.session_state.sheet_created = False
if "sheet_url" not in st.session_state:
    st.session_state.sheet_url = None


@st.cache_data(ttl=300)
def get_available_datasets() -> List[str]:
    """Get available datasets from the database."""
    return cached.load_dataset_uris()


@st.cache_data(ttl=300)
def get_dataset_names() -> Dict[str, str]:
    """Get mapping of dataset URIs to display names."""
    dataset_uris = cached.load_dataset_uris()
    # Create a simple mapping from URI to a display name
    dataset_names = {}
    for uri in dataset_uris:
        # Extract a display name from the URI (e.g., "garden/who/2024-01-01/dataset_name")
        parts = uri.split("/")
        if len(parts) >= 2:
            display_name = f"{parts[-2]} - {parts[-1]}" if len(parts) >= 3 else parts[-1]
        else:
            display_name = uri
        dataset_names[uri] = display_name
    return dataset_names


@st.cache_data(ttl=60)
def load_dataset_tables(dataset_uri: str) -> List[str]:
    """Load table names from a dataset URI."""
    try:
        from owid.catalog import Dataset

        from etl.paths import DATA_DIR

        # Construct the dataset path directly
        dataset_path = DATA_DIR / dataset_uri

        if dataset_path.exists():
            ds = Dataset(dataset_path)
            return list(ds.table_names)
        else:
            st.error(f"Dataset path does not exist: {dataset_path}")
            return []

    except Exception as e:
        st.error(f"Error loading dataset tables: {e}")
        return []


@st.cache_data(ttl=60)
def load_table_data(dataset_uri: str, table_name: str) -> Optional[pd.DataFrame]:
    """Load data from a specific table."""
    try:
        from owid.catalog import Dataset

        from etl.paths import DATA_DIR

        # Construct the dataset path directly
        dataset_path = DATA_DIR / dataset_uri

        if dataset_path.exists():
            ds = Dataset(dataset_path)
            tb = ds.read(table_name)
            table_name = tb.metadata.title
            df = pd.DataFrame(tb)
            return df
        else:
            st.error(f"Dataset path does not exist: {dataset_path}")
            return None

    except Exception as e:
        st.error(f"Error loading table: {e}")
        return None


def create_sheet_from_data(df: pd.DataFrame, sheet_title: str) -> tuple[str, str] | tuple[None, None]:
    """Create a public read-only Google Sheet from DataFrame."""
    try:
        # Create sheet
        sheet = GoogleSheet.create_sheet(title=sheet_title)

        # Write data
        sheet.write_dataframe(df)

        # Make public and read-only
        drive = GoogleDrive()
        drive.set_file_permissions(file_id=sheet.sheet_id, role="reader", general_access="anyone")

        return sheet.url, sheet.sheet_id
    except Exception as e:
        st.error(f"Error creating Google Sheet: {e}")
        return None, None


# Main interface
datasets: List[str] = get_available_datasets()
dataset_display_names: Dict[str, str] = get_dataset_names()

if not datasets:
    st.error("No datasets available.")
    st.stop()

# Dataset selection
col1, col2 = st.columns(2)

with col1:
    selected_dataset_uri: Optional[str] = st.selectbox(
        label="Select Dataset",
        options=datasets,
        format_func=lambda uri: dataset_display_names.get(uri, uri),
        help="Choose the dataset you want to export to Google Sheets",
    )

if selected_dataset_uri:
    # Load tables for selected dataset
    table_names = load_dataset_tables(selected_dataset_uri)

    with col2:
        if table_names:
            selected_table = st.selectbox(
                label="Select Table", options=table_names, help="Choose the table you want to export"
            )
        else:
            st.error("No tables found in selected dataset.")
            st.stop()

    # Show dataset info
    with st.expander("Dataset Information", expanded=False):
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.write(f"**Dataset:** {dataset_display_names.get(selected_dataset_uri, selected_dataset_uri)}")
            st.write(f"**URI:** {selected_dataset_uri}")
        with col_info2:
            st.write(f"**Tables Available:** {len(table_names)}")

    # Load and preview data
    if selected_table:
        df = load_table_data(selected_dataset_uri, selected_table)

        if df is not None:
            # Data preview
            st.subheader("Data Preview")
            st.write(f"**Rows:** {len(df):,} | **Columns:** {len(df.columns):,}")

            # Show first few rows
            st.dataframe(df.head(100), use_container_width=True)

            if len(df) > 100:
                st.info(f"Showing first 100 rows. Full dataset has {len(df):,} rows.")

            # Sheet creation options
            st.subheader("Export Options")

            col_options1, col_options2 = st.columns(2)

            with col_options1:
                sheet_title = st.text_input(
                    "Sheet Title",
                    value=f"{dataset_display_names.get(selected_dataset_uri, selected_dataset_uri)} - {selected_table}",
                    help="Title for the Google Sheet",
                )

            with col_options2:
                max_rows = st.number_input(
                    "Maximum Rows to Export",
                    min_value=1,
                    max_value=len(df),
                    value=min(len(df), len(df)),
                    help="Limit the number of rows to export (Google Sheets has limits)",
                )

            # Create sheet button
            create_button = st.button(
                "🚀 Create Public Google Sheet",
                type="primary",
                use_container_width=True,
                disabled=st.session_state.sheet_created,
            )

            if create_button:
                with st.spinner("Creating Google Sheet..."):
                    # Limit data if necessary
                    if max_rows < len(df):
                        export_df = df.head(max_rows)
                    else:
                        export_df = df.copy()

                    # Explicit assertion that we have a DataFrame
                    assert isinstance(export_df, pd.DataFrame), "Expected DataFrame"

                    # Create sheet
                    sheet_url, sheet_id = create_sheet_from_data(export_df, sheet_title)

                    if sheet_url:
                        st.session_state.sheet_created = True
                        st.session_state.sheet_url = sheet_url
                        st.session_state.sheet_id = sheet_id
                        st.rerun()

            # Show created sheet info
            if st.session_state.sheet_created and st.session_state.sheet_url:
                st.success("✅ Google Sheet created successfully!")

                col_result1, col_result2 = st.columns(2)

                with col_result1:
                    st.markdown(f"**🔗 [Open Google Sheet]({st.session_state.sheet_url})**")
                    st.code(st.session_state.sheet_url, language="text")

                with col_result2:
                    st.markdown("**Sheet Details:**")
                    st.write(f"• Title: {sheet_title}")
                    st.write(f"• Rows exported: {min(max_rows, len(df)):,}")
                    st.write(f"• Columns: {len(df.columns):,}")
                    st.write("• Access: Public (read-only)")

                # Reset button
                if st.button("Create Another Sheet", type="secondary"):
                    st.session_state.sheet_created = False
                    st.session_state.sheet_url = None
                    st.rerun()

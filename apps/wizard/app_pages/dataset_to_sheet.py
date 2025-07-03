"""
Dataset to Google Sheet Exporter

Allows users to select a dataset and table, then creates a public read-only Google Sheet.
"""

from typing import Dict, List, Optional, Union

import pandas as pd
import streamlit as st

from apps.utils.google import GoogleDrive, GoogleSheet
from apps.wizard.app_pages.dataset_preview.utils import get_datasets
from etl.config import OWID_ENV

# Page config
st.set_page_config(page_title="Dataset to Google Sheet", page_icon="📊", layout="wide")

st.title("📊 Dataset to Google Sheet Exporter")
st.markdown("Select a dataset and table to create a public read-only Google Sheet.")

# Initialize session state
if "sheet_created" not in st.session_state:
    st.session_state.sheet_created = False
if "sheet_url" not in st.session_state:
    st.session_state.sheet_url = None


def prompt_dataset_options(dataset_options: List[int], datasets: Dict) -> Optional[int]:
    """Ask user which dataset they want with query param syncing."""
    # Update query params if dataset is selected
    if "dataset_select" in st.session_state:
        st.query_params["datasetId"] = str(st.session_state["dataset_select"])

    # Collect Query params
    dataset_id = st.query_params.get("datasetId")
    if dataset_id == "None":
        dataset_id = None

    # Correct dataset id
    if dataset_id is None:
        dataset_index = None
        dataset_options = [dataset_id for dataset_id in dataset_options if datasets[dataset_id]["isArchived"] == 0]
    else:
        dataset_id = int(dataset_id)
        if dataset_id in dataset_options:
            if datasets[dataset_id]["isArchived"] == 0:
                dataset_options = [
                    dataset_id for dataset_id in dataset_options if datasets[dataset_id]["isArchived"] == 0
                ]
            dataset_index = dataset_options.index(dataset_id)
        else:
            st.error(f"Dataset with ID {dataset_id} not found. Please review the URL query parameters.")
            dataset_index = None
            dataset_options = [dataset_id for dataset_id in dataset_options if datasets[dataset_id]["isArchived"] == 0]

    # Show dropdown with options
    dataset_id = st.selectbox(
        label="Dataset",
        options=dataset_options,
        format_func=lambda x: datasets[x]["display_name"],
        key="dataset_select",
        placeholder="Select dataset",
        index=dataset_index,
        help="By default, only non-archived datasets from ETL are shown. However, if you search for an archived (or pre-ETL) one via QUERY PARAMS, the list will show all datasets. To use QUERY PARAMS, add `?datasetId=YOUR_DATASET_ID` to the URL.",
    )

    if dataset_id is not None:
        return int(dataset_id)
    return None


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
def load_table_data(dataset_uri: str, table_name: str) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    """Load data from a specific table and return DataFrame with metadata title."""
    try:
        from owid.catalog import Dataset

        from etl.paths import DATA_DIR

        # Construct the dataset path directly
        dataset_path = DATA_DIR / dataset_uri

        if dataset_path.exists():
            ds = Dataset(dataset_path)
            tb = ds.read(table_name)

            # Get the metadata title for the sheet title
            dataset_title = ds.metadata.title or "Dataset"
            table_short_name = tb.metadata.short_name or table_name
            sheet_title = f"{dataset_title} - {table_short_name}"

            # Ensure we get a proper DataFrame
            df = pd.DataFrame(tb)
            # Reset index to flatten multi-index if present
            df = df.reset_index(drop=True)

            return df, sheet_title
        else:
            st.error(f"Dataset path does not exist: {dataset_path}")
            return None, None

    except Exception as e:
        st.error(f"Error loading table: {e}")
        return None, None


def create_sheet_from_data(df: pd.DataFrame, sheet_title: str) -> Union[tuple[str, str], tuple[None, None]]:
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


# Get datasets from DB / cached (using the same function as dataset preview)
DATASETS = get_datasets()

# Get dataset options
dataset_options = list(DATASETS.keys())

# Show dataset search bar with enhanced functionality
DATASET_ID = prompt_dataset_options(dataset_options, DATASETS)

if DATASET_ID is not None:
    dataset = DATASETS[DATASET_ID]

    # Show dataset header with admin link
    st.header(f"[{dataset['name']}]({OWID_ENV.dataset_admin_site(DATASET_ID)})")

    # Show dataset metadata
    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        st.markdown(f"**Dataset ID:** {DATASET_ID}")
        st.markdown(f"**Last Updated:** {dataset['updatedAt'].strftime('%Y-%m-%d')}")
    with col_info2:
        if dataset["isPrivate"] == 1:
            st.markdown("🔒 **Private dataset**")
        if dataset["isArchived"] == 1:
            st.markdown("🗃️ **Archived dataset**")
    with col_info3:
        if dataset.get("catalogPath"):
            st.markdown(f"**Catalog Path:** {dataset['catalogPath']}")

    # Only proceed if dataset has a catalog path (ETL dataset)
    if dataset.get("catalogPath"):
        dataset_uri = f"garden/{dataset['catalogPath']}"

        # Load tables for selected dataset
        table_names = load_dataset_tables(dataset_uri)

        if table_names:
            # Table selection
            selected_table = st.selectbox(
                label="Select Table",
                options=table_names,
                help="Choose the table you want to export",
            )

            # Load and preview data
            if selected_table:
                df, default_sheet_title = load_table_data(dataset_uri, selected_table)

                if df is not None and default_sheet_title is not None:
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
                            value=default_sheet_title,
                            help="Title for the Google Sheet",
                        )

                    with col_options2:
                        max_rows = st.number_input(
                            "Maximum Rows to Export",
                            min_value=1,
                            max_value=len(df),
                            value=min(10000, len(df)),
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
                            # Limit data if necessary and ensure it's a DataFrame
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
        else:
            st.error("No tables found in selected dataset.")
    else:
        st.warning("This dataset doesn't have a catalog path. It might be a pre-ETL dataset that can't be exported.")

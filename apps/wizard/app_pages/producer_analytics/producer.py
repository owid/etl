import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder

from apps.wizard.app_pages.producer_analytics.charts import get_producer_charts_analytics
from apps.wizard.app_pages.producer_analytics.utils import columns_producer


@st.cache_data(show_spinner=False)
def get_producer_analytics_per_producer(min_date, max_date, excluded_steps):
    # Load the steps dataframe with producer data and analytics.
    df_expanded = get_producer_charts_analytics(min_date=min_date, max_date=max_date, excluded_steps=excluded_steps)

    # st.toast("⌛ Adapting the data for presentation...")
    # Group by producer and get the full list of chart slugs for each producer.
    df_grouped = df_expanded.groupby("producer", observed=True, as_index=False).agg(
        {
            "grapher": lambda x: [item for item in x if pd.notna(item)],  # Filter out NaN values
            "renders_365d": "sum",
            "renders_30d": "sum",
            "renders_custom": "sum",
        }
    )
    df_grouped["n_charts"] = df_grouped["grapher"].apply(len)

    # Check if lists are unique. If not, make them unique in the previous line.
    error = "Duplicated chart slugs found for a given producer."
    assert df_grouped["grapher"].apply(lambda x: len(x) == len(set(x))).all(), error

    # Drop unnecessary columns.
    df_grouped = df_grouped.drop(columns=["grapher"])

    # Sort conveniently.
    df_grouped = df_grouped.sort_values(["renders_custom"], ascending=False).reset_index(drop=True)

    return df_grouped


def show_producers_grid(df_producers, min_date, max_date):
    """Show table with producers analytics."""
    gb = GridOptionsBuilder.from_dataframe(
        df_producers,
    )
    gb.configure_grid_options(
        # domLayout="autoHeight",
        enableCellTextSelection=True,
    )
    gb.configure_selection(
        selection_mode="multiple",
        use_checkbox=True,
        rowMultiSelectWithClick=True,
        suppressRowDeselection=False,
        groupSelectsChildren=True,
        groupSelectsFiltered=True,
    )
    gb.configure_default_column(
        editable=False,
        groupable=True,
        sortable=True,
        filterable=True,
        resizable=True,
    )

    # Enable column auto-sizing for the grid.
    gb.configure_grid_options(suppressSizeToFit=False)  # Allows dynamic resizing to fit.
    gb.configure_default_column(autoSizeColumns=True)  # Ensures all columns can auto-size.

    # Configure individual columns with specific settings.
    COLUMNS_PRODUCERS = columns_producer(min_date, max_date)
    for column in COLUMNS_PRODUCERS:
        gb.configure_column(column, **COLUMNS_PRODUCERS[column])
    # Configure pagination with dynamic page size.
    gb.configure_pagination(
        # paginationAutoPageSize=False,
        # paginationPageSize=40,
    )
    # Build the grid options.
    grid_options = gb.build()
    # Custom CSS to ensure the table stretches across the page.
    custom_css = {
        ".ag-theme-streamlit": {
            "max-width": "100% !important",
            "width": "100% !important",
            "margin": "0 auto !important",  # Centers the grid horizontally.
        },
    }
    # Display the grid table with the updated grid options.
    grid_response = AgGrid(
        data=df_producers,
        gridOptions=grid_options,
        height=500,
        width="100%",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        fit_columns_on_grid_load=True,  # Automatically adjust columns when the grid loads.
        allow_unsafe_jscode=True,
        theme="streamlit",
        custom_css=custom_css,
        # excel_export_mode=ExcelExportMode.MANUAL,  # Doesn't work?
    )

    # Get the selected producers from the first table.
    df = grid_response["selected_rows"]
    if df is None:
        return []

    producers_selected = df["producer"].tolist()
    return producers_selected
